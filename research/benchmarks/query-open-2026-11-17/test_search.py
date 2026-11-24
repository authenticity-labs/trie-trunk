#!/usr/bin/env python3
"""Search through the packaged native WebKit app against a sandbox Unix socket."""
import argparse
import http.client
import http.server
import json
import os
import hashlib
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('app', type=Path)
    upstream = parser.add_mutually_exclusive_group(required=True)
    upstream.add_argument('--socket')
    upstream.add_argument('--endpoint', help='Existing HTTP(S) data service')
    parser.add_argument('--window', choices=['60', '1440', '86400'], default='1440')
    parser.add_argument('--scope', default='', help='Logical cache/service/resource scope')
    parser.add_argument('--query', default='events | where duration >= 10000')
    parser.add_argument('--from', dest='window_from', help='Explicit UTC start (paired with --to)')
    parser.add_argument('--to', dest='window_to', help='Explicit UTC end; disables now()')
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--output', type=Path, help='Save the native report and optional proxy request audit')
    parser.add_argument('--expect-summary-pruned', action='store_true', help='Verify summaries disable Search before an event scan')
    parser.add_argument('--expect-min-matches', type=int, default=0)
    parser.add_argument('--expect-table-total', type=int, help='Verify the sum of the final visible SQL result column')
    parser.add_argument('--existing-origin', type=int, help='Use an existing local origin and normal app data identity; its app must be closed')
    parser.add_argument('--trace', action='store_true', help='Report native test startup milestones through an independent local test endpoint')
    parser.add_argument('--verify-profile', action='store_true', help='Verify Profile tab responsiveness and real search after navigation')
    parser.add_argument('--verify-dashboard', action='store_true', help='Verify summary collection and charts in an isolated test profile')
    parser.add_argument('--direct-socket', action='store_true', help='Measure the real bridge/socket path without the inspecting Python proxy')
    parser.add_argument('--dispatch', choices=['port', 'worker', 'worker-duplex'], default='port', help='Compare MessagePort, host Worker requests, and host Worker requests/replies')
    args = parser.parse_args()
    endpoint = urlsplit(args.endpoint) if args.endpoint else None
    if endpoint and (endpoint.scheme not in ('http', 'https') or not endpoint.hostname or endpoint.path not in ('', '/') or endpoint.query or endpoint.fragment):
        parser.error('--endpoint must be an HTTP(S) origin')
    if args.direct_socket and not args.socket:
        parser.error('--direct-socket requires --socket')
    if bool(args.window_from) != bool(args.window_to) or args.repeat < 1:
        parser.error('--from and --to must be paired; --repeat must be positive')
    app = args.app.resolve()
    failures = []
    completed = []
    with tempfile.TemporaryDirectory(prefix='snoot-search-test-') as temporary:
        root = Path(temporary)
        resources = root / 'resources'
        shutil.copytree(app / 'Contents/Resources', resources)
        config_path = resources / 'neutralino.config.json'
        config = json.loads(config_path.read_text())
        if not args.existing_origin:
            config.update(dataLocation='app', storageLocation='app', applicationId='com.authenticitylabs.snoot.search-test')
        config_path.write_text(json.dumps(config))
        web = resources / 'web'
        result_path = web / 'search-test-result.json'
        result_path.write_text('{}')

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                if self.path == '/__test/progress':
                    print('Native Progress: ' + body.decode('utf-8', errors='replace'), flush=True)
                    self.send_response(204)
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    return
                if endpoint:
                    connection = http.client.HTTPSConnection if endpoint.scheme == 'https' else http.client.HTTPConnection
                    upstream = connection(endpoint.hostname, endpoint.port, timeout=30)
                else:
                    upstream = http.client.HTTPConnection('localhost', timeout=30)
                    upstream.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    upstream.sock.connect(args.socket)
                try:
                    started = time.perf_counter()
                    started_at = time.time() * 1000
                    headers = {k: v for k, v in self.headers.items() if k.lower() not in ('host', 'connection')}
                    upstream.request(self.command, self.path, body=body, headers=headers)
                    response = upstream.getresponse()
                    headers_ms = (time.perf_counter() - started) * 1000
                    payload = response.read()
                    if (self.headers.get('Trunk-Debug') or response.getheader('Trunk-Timing') or self.path == '/diagnostics/searches'):
                        failures.append({'diagnosticsWhenOff': self.path.split('?')[0]})
                    completed.append({"path":self.path.split("?")[0],"target":self.path,
                        "startedAt":started_at,"headersMs":headers_ms,
                        "upstreamMs":(time.perf_counter()-started)*1000,
                        "bytes":len(payload),"status":response.status})
                    if response.status >= 400:
                        failures.append({'path': self.path, 'status': response.status,
                                         'detail': payload[:1024].decode('utf-8', errors='replace')})
                    self.send_response(response.status)
                    for k, v in response.getheaders():
                        if k.lower() not in ('transfer-encoding', 'content-length', 'connection'):
                            self.send_header(k, v)
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers()
                    if self.command != 'HEAD':
                        self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    # Calendar changes and native window shutdown cancel requests.
                    pass
                finally:
                    upstream.close()

            do_POST = do_GET
            do_HEAD = do_GET

        class ConcurrentProxy(http.server.ThreadingHTTPServer):
            # Match the native server's concurrent acceptance; socketserver's
            # default backlog of five artificially rejects a resource fan-out.
            request_queue_size = 128
        proxy = ConcurrentProxy(('127.0.0.1', 0), Proxy)
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        # This test supplies its own driver instead of the general storage self-test.
        index = web / 'index.html'
        setup = ''
        setup += '<script>sessionStorage.setItem("snoot.dispatch",' + json.dumps(args.dispatch) + ')</script>'
        if args.window_from or args.scope:
            workspace = dict(version=1, page='Search', collapsed=False, tap=args.scope, range='custom' if args.window_from else args.window,
                             **{'from': (args.window_from or '').removesuffix('Z'), 'to': (args.window_to or '').removesuffix('Z')},
                             localParameters={}, localTap='', localFrom='', localTo='', pageSize=100, scroll=0)
            setup += '<script>sessionStorage.setItem("snoot:workspace",' + json.dumps(json.dumps(workspace)) + ')</script>'
        if args.trace:
            setup += '<script>localStorage.setItem("snoot.search.debug","1");globalThis.__testProgress=value=>fetch(' + json.dumps(f'http://127.0.0.1:{proxy.server_port}/__test/progress') + ',{method:"POST",mode:"no-cors",body:JSON.stringify(value)});__testProgress({stage:"head"});</script>'
        else:
            setup += '<script>globalThis.__testProgress=()=>{};</script>'
        setup += '''<script>
window.addEventListener('error',event=>{__testProgress({stage:'error',message:event.message});fetch('/__desktop/report',{method:'POST',body:JSON.stringify({ok:false,stage:'page-error',error:event.message,file:event.filename,line:event.lineno})})});
window.addEventListener('unhandledrejection',event=>{__testProgress({stage:'rejection',message:String(event.reason)});fetch('/__desktop/report',{method:'POST',body:JSON.stringify({ok:false,stage:'unhandled-rejection',error:String(event.reason)})})});
document.addEventListener('DOMContentLoaded',()=>__testProgress({stage:'dom-ready'}));
if(TEST_TRACE)for(const ms of [100,1000,5000])setTimeout(()=>__testProgress({stage:'timer',ms,ready:document.readyState,screen:document.body?.innerText.slice(-1200)}),ms);
setTimeout(()=>{__testProgress({stage:'watchdog',url:location.href,ready:document.readyState,screen:document.body?.innerText.slice(-4000)});fetch('/__desktop/report',{method:'POST',body:JSON.stringify({ok:false,stage:'watchdog',url:location.href,ready:document.readyState,screen:document.body?.innerText.slice(-4000),scripts:[...document.scripts].map(s=>s.src)})})},50000);
</script>'''
        setup=setup.replace('TEST_TRACE', 'true' if args.trace else 'false')
        index.write_text(index.read_text().replace('<head>', '<head>' + setup).replace('</head>', '<script defer src="/search-test.js"></script></HEAD>'))
        driver = Path(__file__).with_name('test_dashboard.js' if args.verify_dashboard else 'test_profile.js' if args.verify_profile else 'test_search.js').read_text()
        driver = driver.replace('SEARCH_QUERY', json.dumps(args.query)).replace('WINDOW_PRESET', json.dumps('custom' if args.window_from else args.window)).replace('MIN_MATCHES', str(args.expect_min_matches)).replace('REPEAT_COUNT', str(args.repeat)).replace('EXPECTED_TABLE_TOTAL', json.dumps(args.expect_table_total)).replace('EXPECT_SUMMARY_PRUNED', json.dumps(args.expect_summary_pruned))
        (web / 'search-test.js').write_text(driver)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = args.existing_origin or listener.getsockname()[1]
        try:
            host = {'cpus': os.cpu_count(), 'loadBefore': os.getloadavg(),
                    'runtime': hashlib.sha256((app / 'Contents/MacOS/neutralino').read_bytes()).hexdigest(),
                    'web': hashlib.sha256((app / 'Contents/Resources/web/index.html').read_bytes()).hexdigest(),
                    'diagnosticLibrary': os.environ.get('DYLD_INSERT_LIBRARIES'),
                    'diagnosticActivity': bool(os.environ.get('SNOOT_TEST_ACTIVITY'))}
            run = subprocess.run([str(app / 'Contents/MacOS/snoot'), '--self-test', '--resources', str(resources),
                                  '--state-dir', str(root / 'profile'), '--port', str(port),
                                  *(['--transport','socket','--socket',args.socket] if args.direct_socket else ['--transport','http']),
                                  '--endpoint', f'http://127.0.0.1:{proxy.server_port}'],
                                 capture_output=True, text=True, timeout=70)
            host['loadAfter'] = os.getloadavg()
            print(run.stdout.strip())
            if args.output:
                args.output.write_text(json.dumps({'native':run.stdout.strip(), 'requests':completed, 'failures':failures,
                    'directSocket':args.direct_socket, 'from':args.window_from, 'to':args.window_to,
                    'query':args.query,'dispatch':args.dispatch,'host':host}, indent=2))
            if failures:
                raise RuntimeError(json.dumps(failures))
            if run.returncode:
                raise RuntimeError(run.stderr + '\n' + json.dumps({'failures':failures,'completed':completed[-60:]}))
        finally:
            proxy.shutdown()
            proxy.server_close()


if __name__ == '__main__':
    main()
