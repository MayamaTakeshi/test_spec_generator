const mysql      = require('mysql')
const tsg = require('../src/index.js')
var request = require('request');

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

var servers = [
	{	
		name: "mysql_server1",
		type: "mysql",
		host: '127.0.0.1',
		port: MYSQL_PORT,
	},
	{	
		name: "http_server1",
		type: "http",
		host: '127.0.0.1',
		port: HTTP_PORT,
		replies: [
			{
				expect: {
					url: '/blablabla',
				},
				data: {
					status: 200,
					headers: {
						'Content-Type': 'application/json',
					},
					body: {
						id: 10,
						name: 'aaa',
					},
				}
			},
			{
				expect: {},
				data: {
					status: 202,
					headers: {
						'Content-Type': 'text/plain',
					},
					body: 'this is plain text',
				}
			},

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

	},
	(conn, connection_name, query) => {
		return null;
	},
	() => {
	}
)

