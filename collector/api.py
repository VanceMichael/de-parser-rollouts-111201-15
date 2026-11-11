import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .storage import Store


def serve(database, port):
    Path(database).parent.mkdir(parents=True, exist_ok=True)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != '/records':
                status = 404
                data = json.dumps({'error': '资源不存在'}, ensure_ascii=False).encode('utf-8')
            else:
                status = 200
                store = Store(database)
                try:
                    data = json.dumps(store.records(), ensure_ascii=False).encode('utf-8')
                finally:
                    store.close()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    print(f'采集记录服务启动于 127.0.0.1:{server.server_port}', flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
