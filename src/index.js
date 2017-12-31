const mqc = require('mysql_query_collector')
const http = require('http')
const textBody = require('body')
const anyBody = require('body/any')

const m = require('data-matching');
const sm = require('string-matching')


const _ = require('lodash')

const atom = require('sexp_builder').atom
const build_sexp = require('sexp_builder').build

const _collected_data = {}

const PARSEABLE_CONTENT_TYPES = ['application/json', 'application/x-www-form-urlencoded']

const traverseTemplate = require('traverse-template')

const dgram = require('dgram')

var debug_print = (s) => {
	console.error(s)
}

var print = (s) => {
	console.log(s)
}

var get_select_fields = (query) => {
	var start = 6 // length of "select"
	var end = query.indexOf(" from ")
	var str = query.substring(start, end)
	return str.split(",").map((s) => { return s.trim() })
}

var get_record_id_field = (query) => {
	var start = query.indexOf(" where ") + 7
	var end = query.indexOf("=")
	return query.substring(start, end).trim()
}

var get_record_id_value = (query) => {
	var start = query.indexOf("=") + 1
	var str = query.substring(start).trim()
	if(str.startsWith("'") && str.endsWith("'")) {
		str = str.slice(1,-1)
	}
	return str
}

var gen_fake_row = (record_id_field, record_id_value, fields) => {
	var counter = 1
	var row = []
	fields.forEach((f) => {
		if(f == record_id_field) {
			row.push(record_id_value)
		} else {
			row.push(counter.toString())
		}
		counter++
	})

	return row
}

var convert_dbquery_reply_to_sexp = (reply) => {
	switch(reply.type) {
	case 'error':
			return build_sexp([atom('error'), reply.errno, reply.message])
			break
	case 'ok':
			return 'ok'
			break
	case 'dataset':
			return build_sexp([atom('dataset'), reply.fields, reply.rows])
			break
	}
}

var print_wait_dbquery_request = (format, server_name, query, reply) => {
	switch(format) {
	case 'xml':
		print('')
		print(`<WaitAndReply>`)
		print(`<DbQuery server_name="${server_name}">${query}</DbQuery>`)
		print(`<Reply>${JSON.stringify(reply)}</Reply>`)
		print(`</WaitAndReply>`)
		break
	case 'sexp':
		print('')
		print(build_sexp([
			atom('WaitAndReply'),
			[
				atom('DbQuery'),
				server_name,
				query
			],
			[
				atom('Reply'),
				reply 
			]
		]))
		break
	}
}

var print_wait_http_request = (format, server, request, reply) => {
	switch(format) {
	case 'xml':
		print('')
		print(`<WaitAndReply>
<HttpRequest server_name="${server.name}">${JSON.stringify(request)}</HttpRequest>
<Reply>${JSON.stringify(reply)}</Reply>
</WaitAndReply>`)
		break
	case 'sexp':
		print('')
		print(build_sexp([
			atom('WaitAndReply'), 
			[
				atom('HttpRequest'),
				server.name,
				request
			],
			[
				atom('Reply'),
				reply
			]
		]))
		break
	}
}

var send_http_reply = (res, reply) => {
	traverseTemplate(reply, _collected_data)

	res.writeHead(reply.status, reply.headers)
	if(_.some(reply.headers, (v,k) => v.toLowerCase() == 'application/json')) {
		res.end(JSON.stringify(reply.body))
	} else {
		res.end(reply.body)
	}
}

var process_http_request = (format, server, req, res, body) => {
	var reply = {
		status: 200,
	}
	var request = {}

	var r = _.find(server.replies, (reply) => {
		return m.partial_match(reply.expect)(req, _collected_data)
	})

	if(r) {
		reply = r.data
		request = r.expect
	}
	print_wait_http_request(format, server, request, reply)

	// This must be done after print_wait_http_request as reply will be modified by send_http_reply 
	send_http_reply(res, reply)
}


var print_wait_udp_request = (format, server, request, reply) => {
	switch(format) {
	case 'xml':
		print('')
		print(`<WaitAndReply>
<UdpRequest server_name="${server.name}">${request}</UdpRequest>
<Reply>${reply}</Reply>
</WaitAndReply>`)
		break
	case 'sexp':
		print('')
		print(build_sexp([
			atom('WaitAndReply'), 
			[
				atom('UdpRequest'),
				server.name,
				request
			],
			[
				atom('Reply'),
				reply
			]
		]))
		break
	}
}

var send_udp_reply = (server, socket, rinfo, reply) => {
	var temp = {
		reply: reply
	}
	traverseTemplate(temp, _collected_data)
	socket.send(temp.reply, rinfo.port, rinfo.address, (err) => {
		if(err) {
			throw `server ${server.name} error when sending ${temp.reply} to ${rinfo.address}:${rinfo.port}: ${err}`
		}
	});
}


var process_udp_request = (format, server, socket, rinfo, msg) => {
	var r = _.find(server.replies, (reply) => {
		return sm.gen_matcher(reply.expect)(msg, _collected_data)
	})

	if(!r) {
		throw `Could not find UDP reply for '${msg}'`
	}

	var reply = r.data
	var request = r.expect

	print_wait_udp_request(format, server, request, reply)

	// This must be done after print_wait_http_request as reply will be modified by send_http_reply 
	send_udp_reply(server, socket, rinfo, reply)
}


module.exports = {
	setup: (format, servers, ready_cb, dbquery_cb) => {
		if(!['xml', 'sexp'].includes(format)) throw `Invalid format ${format}`

		var mysql_servers = servers.filter(s => s.type == 'mysql')

		var http_servers = servers.filter(s => s.type == 'http')

		var udp_servers = servers.filter(s => s.type == 'udp')

		var ready_servers = 0;

		var check_ready = (c) => {
			ready_servers += c
			if(ready_servers == servers.length) {
				ready_cb()
			}
		}

		mqc.setup(mysql_servers, 
			() => {
				check_ready(mysql_servers.length)
			},
			(conn, server_name, query) => {
				//debug_print(`Simulated MySQL server ${server_name} got query: ${query}`)

				var q = query.trim().toLowerCase().replace(/\s\s+/g, ' ')

				var command = q.split(" ")[0]
				//debug_print(`command=${command}`)

				var reply = dbquery_cb(conn, server_name, query);
				if(reply) {
					print_wait_dbquery_request(format, server_name, query, reply)
					return reply
				} else {
					if(['set', 'insert', 'update', 'delete', 'call', 'commit', 'rollback'].includes(command)) {
						reply = {
							type: 'ok',
						}
						print_wait_dbquery_request(format, server_name, query, reply)
						return reply
					} else if (command == "select") {
						var fields = get_select_fields(q)
						var id_field = get_record_id_field(q)
						var id_value = get_record_id_value(q)
						reply = {
							type: 'dataset',
							fields: fields,
							rows: [gen_fake_row(id_field, id_value, fields)],
						}	
						print_wait_dbquery_request(format, server_name, query, reply)
						return reply
					} else {
						debug_print(`Unexpected query`)
						process.exit(1)
					}
				}
			}
		)

		http_servers.forEach(server => {
			var s = http.createServer((req, res) => {
				if(req.headers['content-type'] == 'plain/text') {
					textBody(req, res, {}, (err, body) => {
						if(err) throw err
						process_http_request(format, server, req, res, body)
					})
				} else if(PARSEABLE_CONTENT_TYPES.includes(req.headers['content-type'])) {
					anyBody(req, res, {}, (err, body) => {
						if(err) throw err
						process_http_request(format, server, req, res, body) 
					})
				} else {
					process_http_request(format, server, req, res, null)
				}
			})
			s.listen({
				host: server.address,
				port: server.port,
			})
			s.on('error', function (e) {
				throw e
			});
			s.on('listening', function (e) {
				debug_print(`HTTP server ${server.name} created. Listening ${server.host}:${server.port}`)
				check_ready(1)
			});
		})

		udp_servers.forEach(server => {
			var socket = dgram.createSocket('udp4');

			socket.on('error', (err) => {
				throw `server error:\n${err.stack}`
			});

			socket.on('message', (msg, rinfo) => {
				msg = msg.toString()
				debug_print(`server got: ${msg} (${typeof msg}) from ${rinfo.address}:${rinfo.port}`)
				process_udp_request(format, server, socket, rinfo, msg)
			});

			socket.on('listening', () => {
				debug_print(`UDP server ${server.name} listening ${server.host}:${server.port}`)
				check_ready(1)
			});

			socket.bind(server.port, server.host);
		})
	},
}

