var mqc = require('mysql_query_collector')

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

var print_wait_dbquery_request = (format, connection_name, query, reply) => {
	switch(format) {
	case 'xml':
		print('')
		print(`<WaitRequest>`)
		print(`<DbQuery connection_name="${connection_name}">${query}</DbQuery>`)
		print(`<Reply>${JSON.stringify(reply)}</Reply>`)
		print(`</WaitRequest>`)
		break
	case 'sexp':
		print('\t\t(WaitRequest')
		print('\t\t\t(DbQuery ' + `"${connection_name}" "${query}"` + ')')
		print('\t\t\t(Reply ' + `"TO_BE_DONE"` + ')')
		print('\t\t)')
		break
	}
}


module.exports = {
	setup: (format, db_connections, ready_cb, query_cb, http_cb) => {
		if(!['xml', 'sexp'].includes('format')) throw `Invalid format ${format}`

		mqc.setup(db_connections, 
			() => {
				ready_cb()
			},
			(conn, connection_name, query) => {
				//debug_print(`connection ${connection_name} got query: ${query}`)

				var q = query.trim().toLowerCase().replace(/\s\s+/g, ' ')

				var command = q.split(" ")[0]
				//debug_print(`command=${command}`)

				var reply = query_cb(conn, connection_name, query);
				if(reply) {
					print_wait_dbquery_request(format, connection_name, query, reply)
					return reply
				} else {
					if(['set', 'insert', 'update', 'delete', 'call', 'commit', 'rollback'].includes(command)) {
						reply = {
							type: 'ok',
						}
						print_wait_dbquery_request(format, connection_name, query, reply)
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
						print_wait_dbquery_request(format, connection_name, query, reply)
						return reply
					} else {
						debug_print(`Unexpected query`)
						process.exit(1)
					}
				}
			}
		)
	},
}

