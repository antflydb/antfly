import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from qualify_native_cancellation import AbortableRequest


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_POST(self):
        try:
            if self.path == "/upload":
                self.server.entered.set()
                self.rfile.read(int(self.headers["Content-Length"]))
            else:
                self.rfile.read(int(self.headers["Content-Length"]))
                if self.path == "/export":
                    self.send_response(200)
                    self.send_header("Content-Length", str(64 * 1024))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(b"x" * (64 * 1024))
                    self.wfile.flush()
                self.server.entered.set()
                self.rfile.read(1)
            self.server.result = "orderly_eof"
        except ConnectionResetError:
            self.server.result = "tcp_reset"
        except Exception as error:
            self.server.result = f"{type(error).__name__}: {error}"
        finally:
            self.close_connection = True
            self.server.finished.set()


class NativeAbortTests(unittest.TestCase):
    def exercise(self, kind):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.entered = threading.Event()
        server.finished = threading.Event()
        server.result = None
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        request = AbortableRequest("127.0.0.1", server.server_port, 3)
        try:
            connection = request.connection
            if kind == "upload":
                connection.putrequest("POST", "/upload")
                connection.putheader("Content-Length", str(1024 * 1024))
                connection.endheaders()
                connection.send(b"x" * 1024)
            else:
                connection.request("POST", "/" + kind, b"{}")
                if kind == "export":
                    response = request.getresponse()
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.read(1024), b"x" * 1024)
                    # HTTPConnection relinquishes its socket for Connection: close.
                    self.assertIsNone(connection.sock)
            self.assertTrue(
                server.entered.wait(2), "server did not receive the request"
            )
            request.abort()
            self.assertTrue(server.finished.wait(2), "server did not observe reset")
            self.assertEqual(server.result, "tcp_reset")
        finally:
            request.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_partial_upload_is_reset_not_half_closed(self):
        self.exercise("upload")

    def test_waiting_prefill_is_reset_not_half_closed(self):
        self.exercise("prefill")

    def test_response_owned_socket_is_reset_not_half_closed(self):
        self.exercise("export")


if __name__ == "__main__":
    unittest.main()
