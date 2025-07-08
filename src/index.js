const mqc = require('mysql_query_collector')
const http = require('http')

const m = require('data-matching');
const sm = require('string-matching')

const _ = require('lodash')

const symbol = require('sexp_builder').symbol
const build_sexp = require('sexp_builder').build

const _collected_data = {}

const traverseTemplate = require('traverse-template')

const dgram = require('dgram')

const types = require('mysql/lib/protocol/constants/types.js')

const traverse = require('traverse')
const template = require('es6-template')

const querystringParse = require("querystring").parse

const net = require('net')

const nodeRedisProtocol = require('node-redis-protocol')
const redisProto = require('redis-proto')

var _options = {
    interpolate_requests: true,
    interpolate_replies: true,
}

var debug_print = s => {
    console.error(s)
}

var print = s => {
    console.log(s)
}

var convert_bang_to_question = s => {
    return s.replace(/(!{([^}]+)})/g, (all, outer, inner) => "${" + inner + "}")
}

var traverse_interpolating_bangs = (obj, locals) => {
    traverse(obj).forEach(function (value) {
        if (typeof value === 'string') {
            var v = convert_bang_to_question(value)
            this.update(template(v, locals || obj))
        }
    })

    return obj
}

var get_select_fields = query => {
    var start = 6 // length of "select"
    var end = query.indexOf(" from ")
    if(end > 0) {
        var str = query.substring(start, end)
        return str.split(",").map(s => s.trim())
    }

    var fields = query.substring(start).split(",").map(s => s.trim())

    return _.map(fields, f => {
        if(f.startsWith("'") && f.endsWith("'")) {
            return {
                name: f.slice(1, -1),
                type: types.VARCHAR,
            }
        } else if(f.startsWith('"') && f.endsWith('"')) {
            return {
                name: f.slice(1, -1),
                type: types.VARCHAR,
            }
        } else if(f.match(/[.1234567890]+/)) {
            return {
                name: f,
                type: types.FLOAT,
            }
        } else if(f.match(/[1234567890]+/)) {
            return {
                name: f,
                type: types.DECIMAL,
            }
        } else {
            return {
                name: f,
                type: types.VARCHAR,
            }
        }
    })
}

var get_record_id_field = query => {
    if(query.indexOf(" where " ) < 0) return null

    var start = query.indexOf(" where ") + 7
    var end = query.indexOf("=")
    return query.substring(start, end).trim()
}

var get_record_id_value = query => {
    if(query.indexOf(" where " ) < 0) return null

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
    fields.forEach(f => {
        var name = f
        if(f.name) name = f.name

        if(record_id_field && name == record_id_field) {
            row.push(record_id_value)
        } else {
            row.push(counter.toString())
        }
        counter++
    })

    return row
}

var print_wait_init_db_request = (format, server, database_name, reply) => {
    switch(format) {
    case 'xml':
        print('')
        print(`<WaitAndReply server_name="${server.name}">`)
        print(`<InitDb>${database_name}</InitDb>`)
        print(`<Reply>${JSON.stringify(reply)}</Reply>`)
        print(`</WaitAndReply>`)
        break
    case 'sexp':
        print('')
        print(build_sexp([
            symbol('WaitAndReply'),
            server.name,
            [
                symbol('InitDb'),
                database_name
            ],
            [
                symbol('Reply'),
                reply
            ]
        ]))
        break
    }
}

var print_wait_dbquery_request = (format, server, request, reply) => {
    var c_request = {...request}
    if(_options.interpolate_requests) {
        traverse_interpolating_bangs(c_request, _collected_data)
    }

    if(_options.interpolate_replies) {
        traverseTemplate(reply, _collected_data)
    }

    switch(format) {
    case 'xml':
        print('')
        print(`<WaitAndReply server_name="${server.name}">`)
        print(`<DbQuery>${c_request.query}</DbQuery>`)
        print(`<Reply>${JSON.stringify(reply)}</Reply>`)
        print(`</WaitAndReply>`)
        break
    case 'sexp':
        print('')
        print(build_sexp([
            symbol('WaitAndReply'),
            server.name,
            [
                symbol('DbQuery'),
                c_request.query
            ],
            [
                symbol('Reply'),
                reply
            ]
        ]))
        break
    }
}

var print_wait_http_request = (format, server, request, reply) => {
    var c_request = {...request}
    if(_options.interpolate_requests) {
        traverse_interpolating_bangs(c_request, _collected_data)
    }

    if(_options.interpolate_replies) {
        //in case of reply, we must use use the original object instead of a clone to ensure send_*_reply will use the same object
        traverseTemplate(reply, _collected_data)
    }

    switch(format) {
    case 'xml':
        print('')
        print(`<WaitAndReply server_name="${server.name}">
<HttpRequest>${JSON.stringify(c_request)}</HttpRequest>
<Reply>${JSON.stringify(reply)}</Reply>
</WaitAndReply>`)
        break
    case 'sexp':
        print('')
        print(build_sexp([
            symbol('WaitAndReply'),
            server.name,
            [
                symbol('HttpRequest'),
                c_request
            ],
            [
                symbol('Reply'),
                reply
            ]
        ]))
        break
    }
}

var send_http_reply = (res, reply) => {
    var c_reply = {...reply}

    traverseTemplate(c_reply, _collected_data)

    var body;

    if(_.some(reply.headers, (v,k) => v.toLowerCase() == 'application/json')) {
        body = JSON.stringify(c_reply.body)
    } else {
        body = c_reply.body
    }

    if(body) {
        c_reply.headers['content-length'] = body.length
    }
    res.writeHead(c_reply.status, c_reply.headers)

    res.end(body)
}

var process_http_request = (format, server, req, res, body) => {
    req.body = body

    var h = _.find(server.hooks, hook => m.partial_match(hook.match)(req, _collected_data))

    if(!h) {
        throw `Could not resolve HTTP reply for server=${server.name}, url=${req.url} and body=[[${body}]]`
    }

    var request = h.match
    var reply = h.reply

    print_wait_http_request(format, server, request, reply)

    send_http_reply(res, reply)
}


var print_wait_udp_request = (format, server, request, reply) => {
    var c_request = {...request}
    if(_options.interpolate_requests) {
        traverse_interpolating_bangs(c_request, _collected_data)
    }

    if(_options.interpolate_replies) {
        //in case of reply, we must use use the original object instead of a clone to ensure send_*_reply will use the same object
        traverseTemplate(reply, _collected_data)
    }

    switch(format) {
    case 'xml':
        print('')
        print(`<WaitAndReply server_name="${server.name}">
<UdpRequest>${c_request}</UdpRequest>
<Reply>${reply}</Reply>
</WaitAndReply>`)
        break
    case 'sexp':
        print('')
        print(build_sexp([
            symbol('WaitAndReply'),
            server.name,
            [
                symbol('UdpRequest'),
                c_request
            ],
            [
                symbol('Reply'),
                reply
            ]
        ]))
        break
    }
}

var print_wait_redis_request = (format, server, request, reply) => {
    var c_request = {...request}
    if(_options.interpolate_requests) {
        traverse_interpolating_bangs(c_request, _collected_data)
    }

    if(_options.interpolate_replies) {
        //in case of reply, we must use use the original object instead of a clone to ensure send_*_reply will use the same object
        traverseTemplate(reply, _collected_data)
    }

    switch(format) {
    case 'xml':
        print('')
        print(`<WaitAndReply server_name="${server.name}">
<RedisRequest>${c_request}</RedisRequest>
<Reply>${reply}</Reply>
</WaitAndReply>`)
        break
    case 'sexp':
        print('')
        print(build_sexp([
            symbol('WaitAndReply'),
            server.name,
            [
                symbol('RedisRequest'),
                c_request
            ],
            [
                symbol('Reply'),
                reply
            ]
        ]))
        break
    }
}

var send_udp_reply = (server, socket, rinfo, reply) => {
    traverseTemplate(reply, _collected_data)

    socket.send(reply.body, rinfo.port, rinfo.address, err => {
        if(err) {
            throw `server ${server.name} error when sending ${temp.reply} to ${rinfo.address}:${rinfo.port}: ${err}`
        }
    });
}

var send_redis_reply = (socket, reply) => {
    traverseTemplate(reply, _collected_data)

    var data = redisProto.encode(reply.body)
    //console.log("send_redis_reply: data=" + data)
    if(Array.isArray(reply.body)) {
            socket.write("*1\r\n", err => {
                    if(err) {
                            throw `Error when writing TCP packet ${data}: ${err}`
                    }
                    socket.write(data, err => {
                            if(err) {
                                    throw `Error when writing TCP packet ${data}: ${err}`
                            }
                    });
            });
    } else {
            socket.write(data, err => {
                    if(err) {
                            throw `Error when writing TCP packet ${data}: ${err}`
                    }
            });
    }
}

var process_udp_request = (format, server, socket, msg) => {
    var h = _.find(server.hooks, hook => sm.gen_matcher(hook.match)(msg, _collected_data))

    if(!h) {
        throw `Could not resolve udp reply for '${msg}'`
    }

    var request = h.match
    var reply = h.reply

    print_wait_redis_request(format, server, request, reply)

    send_redis_reply(server, socket, reply)
}

var process_redis_request = (format, server, socket, msg) => {
    var h = _.find(server.hooks, hook => sm.gen_matcher(hook.match)(msg, _collected_data))

    if(!h) {
        throw `Could not resolve redis reply for '${msg}'`
    }

    var request = h.match
    var reply = h.reply

    print_wait_redis_request(format, server, request, reply)

    send_redis_reply(server, socket, reply)
}


module.exports = {
    setup: (format, servers, ready_cb, options) => {
        if(!['xml', 'sexp'].includes(format)) throw `Invalid format ${format}`

        if(options) {
            options = _.extend(_options, options)
        }

        var mysql_servers = servers.filter(s => s.type == 'mysql')

        var http_servers = servers.filter(s => s.type == 'http')

        var udp_servers = servers.filter(s => s.type == 'udp')

        var redis_servers = servers.filter(s => s.type == 'redis')

        var ready_servers = 0;

        var check_ready = c => {
            ready_servers += c
            if(ready_servers == servers.length) {
                ready_cb()
            }
        }

        mqc.setup(mysql_servers,
            () => {
                check_ready(mysql_servers.length)
            },
            (conn, server, query) => {
                debug_print(`Simulated MySQL server ${server.name} got query: ${query}`)

                var q = query.trim().toLowerCase().replace(/\s\s+/g, ' ')

                var command = q.split(" ")[0]
                //debug_print(`command=${command}`)

                var request = {
                    query: query
                }

                var reply = null

                var h = _.find(server.hooks, hook => {
                    return m.partial_match(hook.match)(request, _collected_data)
                })

                if(h) {
                    request = h.match
                    reply = h.reply
                }

                if(reply) {
                    print_wait_dbquery_request(format, server, request, reply)
                    return reply
                } else {
                    if(['set', 'insert', 'update', 'delete', 'call', 'commit', 'rollback', 'show', 'replace'].includes(command)) {
                        reply = {
                            type: 'ok',
                        }
                        print_wait_dbquery_request(format, server, request, reply)
                        return reply
                    } else if (command == "select") {
                        var fields = get_select_fields(q)
                        var id_field = get_record_id_field(q)
                        var id_value = get_record_id_value(q)
                        var rows
                        if(id_field) {
                            rows = [gen_fake_row(id_field, id_value, fields)]
                        } else {
                            if(_.every(fields, f => f.name)) {
                                rows = [_.map(fields, f => f.name)]
                            } else {
                                rows = [gen_fake_row(null, null, fields)]
                            }
                        }
                        reply = {
                            type: 'dataset',
                            fields: fields,
                            rows: rows,
                        }
                        print_wait_dbquery_request(format, server, request, reply)
                        return reply
                    } else {
                        debug_print(`Unexpected query '${query}' arrived at server ${server.name}`)
                        process.exit(1)
                    }
                }
            },
            (conn, server, database_name) => {
                var reply = {type: 'ok'}
                print_wait_init_db_request(format, server, database_name, reply)
                return reply
            }
        )

        http_servers.forEach(server => {

            var s = http.createServer((req, res) => {
                var data = ""
                var content_type = req.headers['content-type']

                req.on('data', function(chunk) {
                    data += chunk.toString()
                })

                req.on('end', function() {
                    var body
                    switch(content_type) {
                    case 'application/json':
                        body = JSON.parse(data)
                        break
                    case 'application/x-www-form-urlencoded':
                        body = querystringParse(data)
                        break
                    default:
                        body = data
                        break
                    }
                    process_http_request(format, server, req, res, body)
                })

            })

            s.listen({
                host: server.address,
                port: server.port,
            })

            s.on('error', function (e) {
                throw e
            })

            s.on('listening', function (e) {
                debug_print(`HTTP server ${server.name} created. Listening ${server.host}:${server.port}`)
                check_ready(1)
            })
        })

        udp_servers.forEach(server => {
            var socket = dgram.createSocket('udp4');

            socket.on('error', err => {
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

        redis_servers.forEach(server => {
            var s = net.createServer(socket => {
                var rp = new nodeRedisProtocol.ResponseParser()
                rp.on('response', msg => {
                    debug_print(`server ${server.name} got: ${msg} (${typeof msg}) from ${socket.address}:${socket.port}`)
                    process_redis_request(format, server, socket, msg)
                })

                socket.on('error', err => {
                    throw `server ${server.name} error:\n${err.stack}`
                })

                socket.on('data', data => {
                    rp.parse(data)
                })
            })

            s.listen(server.port, server.host)

            s.on('listening', () => {
                check_ready(1)
            })
        })
    },

    print_wait_dbquery_request: print_wait_dbquery_request,

    print_wait_init_db_request: print_wait_init_db_request,

    print_wait_http_request: print_wait_http_request,

    print_wait_udp_request: print_wait_udp_request,

    print_wait_redis_request: print_wait_redis_request,
}

