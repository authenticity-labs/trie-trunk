"""Test-only Unix-socket API: JSON export/control plus unchanged upstream routes."""
import argparse
import http.client
import http.server
import socket
import socketserver
from pathlib import Path

p=argparse.ArgumentParser()
p.add_argument('--socket',required=True)
p.add_argument('--upstream',required=True)
p.add_argument('--data',type=Path,required=True)
p.add_argument('--mode',type=Path,required=True)
a=p.parse_args()

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def do_GET(self):
        body=self.rfile.read(int(self.headers.get('Content-Length','0')))
        if '__bench=' in self.path: print(self.command,self.path,len(body),flush=True)
        if '__bench=progress' in self.path: payload=b'ok'
        elif self.path.endswith('?__bench=mode'): payload=a.mode.read_bytes()
        elif self.path.endswith('?__bench=events'): payload=a.data.read_bytes()
        elif self.path.endswith('?__bench=export') and self.command=='POST':
            a.data.write_bytes(body)
            payload=b'ok'
        else:
            upstream=http.client.HTTPConnection('localhost',timeout=45)
            upstream.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
            upstream.sock.connect(a.upstream)
            try:
                upstream.request(self.command,self.path,body,{k:v for k,v in self.headers.items() if k.lower() not in ('host','connection')})
                response=upstream.getresponse();payload=response.read()
                self.send_response(response.status)
                for k,v in response.getheaders():
                    if k.lower() not in ('content-length','connection','transfer-encoding'): self.send_header(k,v)
            finally: upstream.close()
            self.send_header('Content-Length',str(len(payload)));self.end_headers()
            if self.command!='HEAD': self.wfile.write(payload)
            return
        self.send_response(200)
        self.send_header('Content-Type','application/json' if self.path.endswith('?__bench=events') else 'text/plain')
        self.send_header('Cache-Control','no-store')
        self.send_header('Content-Length',str(len(payload)));self.end_headers()
        self.wfile.write(payload)
    do_POST=do_GET
    do_HEAD=do_GET

class Server(socketserver.ThreadingMixIn,socketserver.UnixStreamServer):
    daemon_threads=True
    request_queue_size=128
Path(a.socket).unlink(missing_ok=True)
try: Server(a.socket,Handler).serve_forever()
finally: Path(a.socket).unlink(missing_ok=True)
