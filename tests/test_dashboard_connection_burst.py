from http.server import BaseHTTPRequestHandler
import socket
import threading

from pipeline.dashboard_server import DashboardHTTPServer


def test_dashboard_accepts_page_connection_burst_before_dispatch():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = b"asset ready"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = DashboardHTTPServer(("127.0.0.1", 0), Handler)
    clients = []
    worker = None
    try:
        # Hold dispatch until every TCP connection is queued, as when a browser
        # starts its page assets together. This checks capacity, not a time SLA.
        for _ in range(16):
            client = socket.create_connection(server.server_address, timeout=2)
            clients.append(client)
            client.sendall(b"GET /asset HTTP/1.0\r\nHost: localhost\r\n\r\n")
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        for client in clients:
            response = b""
            while True:
                chunk = client.recv(4096)
                if not chunk:
                    break
                response += chunk
            assert response.startswith(b"HTTP/1.0 200")
            assert response.endswith(b"asset ready")
    finally:
        for client in clients:
            client.close()
        if worker is not None:
            server.shutdown()
            worker.join(3)
        server.server_close()
