import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import struct
import tempfile
import threading
import unittest
from native_checkpoint_transport import send_resume

class Reject(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *args): pass
    def do_POST(self):
        header = self.rfile.read(40)
        _, metadata_bytes, _, _, mime_bytes, _, _ = struct.unpack('<8sQIIIIQ', header)
        self.rfile.read(metadata_bytes + mime_bytes)
        self.close_connection = True
        if self.server.response_status is None: return
        body = json.dumps({'error':'INVALID_KV_CHECKPOINT', 'message':'KvCheckpointAppendLimit'}).encode()
        self.send_response(self.server.response_status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

class EarlyRejectionTests(unittest.TestCase):
    def request(self, status):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'transport-only.kv'
            with path.open('wb') as f: f.truncate(128*1024*1024)
            server=ThreadingHTTPServer(('127.0.0.1',0),Reject)
            server.response_status=status
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
            try:
                response=send_resume(connection,'/resume',{'stream':True},path)
                return response.status,json.loads(response.read())
            finally:
                connection.close();server.shutdown();server.server_close();thread.join()
    def test_early_rejection_preserves_status_and_error(self):
        status,result=self.request(400)
        self.assertEqual(status,400)
        self.assertEqual(result['message'],'KvCheckpointAppendLimit')
    def test_premature_success_cannot_accept_partial_upload(self):
        with self.assertRaises((BrokenPipeError,ConnectionResetError)):self.request(200)
    def test_disconnect_without_response_preserves_upload_failure(self):
        with self.assertRaises((BrokenPipeError,ConnectionResetError)):self.request(None)

if __name__=='__main__':unittest.main()
