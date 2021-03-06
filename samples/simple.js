const mysql      = require('mysql')
const tsg = require('../src/index.js')
var request = require('request');

const types = require('mysql/lib/protocol/constants/types.js')

request = request.defaults({'proxy': null})

var usage = (err) => {
	var out = console.log

	if(err) {
		out = console.error
	}

	if(err) {
		out(err)
	}

	out(`
Parameters: format
Ex:         xml

Details: format can be 'xml' or 'sexp'
`)
}

if(process.argv.length != 3) {
	usage("Invalid number of arguments")
	process.exit(1)
}
format = process.argv[2]

const MYSQL_PORT = 6000
const HTTP_PORT  = 6001
const UDP_PORT  = 6002

var servers = [
	{	
		name: "mysql_server1",
		type: "mysql",
		host: '127.0.0.1',
		port: MYSQL_PORT,
		hooks: [
			{
				match: {
					query: "select id, name from user",
				},
				reply: {
					type: "dataset",
					fields: [
						{
							name: "id",
							type: types.LONG,
						},
						{
							name: "name",
							type: types.VARCHAR,
						}
					],
					rows: [[10, "user1"]],
				},
			},
		],
	},
	{	
		name: "http_server1",
		type: "http",
		host: '127.0.0.1',
		port: HTTP_PORT,
		hooks: [
			{
				match: {
					url: '/blablabla?id=!{id}',
				},
				reply: {
					status: 200,
					headers: {
						'Content-Type': 'application/json',
					},
					body: {
						id: '${id}',
						name: 'aaa',
					},
				}
			},
			{
				match: {
				},
				reply: {
					status: 202,
					headers: {
						'Content-Type': 'text/plain',
					},
					body: 'this is plain text',
				}
			},

		]
	},
	{
		name: "udp_server1",
		type: "udp",
		host: "127.0.0.1",
		port: UDP_PORT,
		hooks: [
			{
				match: "!{name}@!{domain}",
				reply: "'${name}' at '${domain}'",
			}
		]
	},
]


tsg.setup(format, 
	servers,
	() => {

		var sync_db_conn = mysql.createConnection({
			host: "127.0.0.1",
			port: MYSQL_PORT
		})

		sync_db_conn.query('select id, name from user', (err) => {
			if(err) throw err

			console.error("Sending POST request")
			var r = request.post(
				`http://127.0.0.1:${HTTP_PORT}/find?name=bla&section=ble`,
				//{ json: { key: 'value' } },
				{ 'form': {"name": "bla bla bla" }},
				function (error, response, body) {
					if (!error && response.statusCode == 200) {
						console.debug(body)
					}
				}
			);
		})
	}
)

