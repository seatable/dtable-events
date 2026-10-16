import io
import json
import socket
import ssl
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, call, patch

import requests
from urllib3.exceptions import ConnectTimeoutError, ReadTimeoutError, SSLError
from urllib3.response import HTTPResponse

from dtable_events.utils import webhook as transport
from dtable_events.webhook.webhook import Webhooker


class WebhookDestinationTest(TestCase):
    def test_rejects_private_and_mixed_answers(self):
        for answers in ([('127.0.0.1', 80)], [('169.254.169.254', 80)],
                        [('8.8.8.8', 80), ('10.0.0.1', 80)]):
            resolved = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', a) for a in answers]
            with self.subTest(answers=answers), patch.object(transport.socket, 'getaddrinfo', return_value=resolved):
                with self.assertRaises(ValueError):
                    transport.resolve_webhook_url('http://receiver.example/hook')

    def test_rejects_ipv6_and_alternative_encodings(self):
        for host in ('[::1]', '[::ffff:127.0.0.1]', '127.1', '2130706433', '0x7f000001'):
            with self.subTest(host=host), self.assertRaises(ValueError):
                transport.resolve_webhook_url('http://' + host)

    def test_each_delivery_rechecks_dns(self):
        public = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('8.8.8.8', 80))]
        private = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 80))]
        with patch.object(transport.socket, 'getaddrinfo', side_effect=[public, private]) as dns, \
                patch.object(transport, 'HTTPConnectionPool') as pool:
            pool.return_value.__enter__.return_value.urlopen.return_value = HTTPResponse(
                body=io.BytesIO(b'{}'), status=200, preload_content=False)
            transport.post_webhook('http://receiver.example/hook', {}, {})
            with self.assertRaises(ValueError):
                transport.post_webhook('http://receiver.example/hook', {}, {})
        self.assertEqual(dns.call_count, 2)
        pool.assert_called_once_with('8.8.8.8', 80)

    def test_https_pool_uses_validated_ip_and_original_hostname(self):
        addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('8.8.8.8', 443))]
        with patch.object(transport.socket, 'getaddrinfo', return_value=addresses) as dns, \
                patch.object(transport, 'HTTPSConnectionPool') as pool:
            connection = pool.return_value.__enter__.return_value
            connection.urlopen.return_value = HTTPResponse(body=io.BytesIO(b'{}'), status=200,
                                                           preload_content=False)
            transport.post_webhook('https://receiver.example/hook', {}, {})
        dns.assert_called_once()
        self.assertEqual(pool.call_args.args, ('8.8.8.8', 443))
        self.assertEqual(pool.call_args.kwargs['server_hostname'], 'receiver.example')
        self.assertEqual(pool.call_args.kwargs['assert_hostname'], 'receiver.example')
        self.assertEqual(pool.call_args.kwargs['cert_reqs'], 'CERT_REQUIRED')
        self.assertEqual(connection.urlopen.call_args.kwargs['headers']['Host'], 'receiver.example')

    def test_connection_falls_back_only_to_validated_addresses(self):
        addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('8.8.8.8', 80)),
                     (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('1.1.1.1', 80))]
        with patch.object(transport.socket, 'getaddrinfo', return_value=addresses), \
                patch.object(transport, 'HTTPConnectionPool') as pool:
            connection = pool.return_value.__enter__.return_value
            connection.urlopen.side_effect = [ConnectTimeoutError('unreachable'),
                HTTPResponse(body=io.BytesIO(b'{}'), status=200, preload_content=False)]
            self.assertEqual(transport.post_webhook('http://receiver.example/hook', {}, {}).status_code, 200)
        self.assertEqual(pool.call_args_list, [call('8.8.8.8', 80), call('1.1.1.1', 80)])

    def test_read_timeout_does_not_resend_post(self):
        addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('8.8.8.8', 80)),
                     (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('1.1.1.1', 80))]
        with patch.object(transport.socket, 'getaddrinfo', return_value=addresses), \
                patch.object(transport, 'HTTPConnectionPool') as pool:
            pool.return_value.__enter__.return_value.urlopen.side_effect = ReadTimeoutError(
                None, '/hook', 'timed out')
            with self.assertRaises(requests.exceptions.ReadTimeout):
                transport.post_webhook('http://receiver.example/hook', {}, {})
        pool.assert_called_once_with('8.8.8.8', 80)


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        self.server.bodies.append(body)
        self.server.events.append((self.command, self.path, self.headers.get('Host'),
                                   self.headers.get('X-SeaTable-Signature'), json.loads(body)))
        self.send_response(self.server.redirect_status if self.path == '/redirect' else 200)
        if self.path == '/redirect':
            self.send_header('Location', '/target')
        self.send_header('Content-Length', '2')
        self.end_headers()
        self.wfile.write(b'{}')

    def do_GET(self):
        self.server.events.append((self.command, self.path))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


class WebhookTransportTest(TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
        self.server.events = []
        self.server.bodies = []
        self.server.redirect_status = 302
        self.port = self.server.server_port
        self.addresses = socket.getaddrinfo('127.0.0.1', self.port, type=socket.SOCK_STREAM)
        self.body = {'event': 'update', 'data': {'value': '测试'}}
        self.headers = {'X-SeaTable-Signature': 'test-signature'}

    def start_server(self):
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def send(self, scheme='http', host='receiver.example', path='/hook'):
        def resolve_ip(host, port, *args, **kwargs):
            # urllib3 may resolve the numeric IP, but must never resolve the URL hostname again.
            self.assertEqual(host, '127.0.0.1')
            self.assertEqual(port, self.port)
            return self.addresses

        with patch.object(transport, 'resolve_webhook_url', return_value=self.addresses), \
                patch.object(transport.socket, 'getaddrinfo', side_effect=resolve_ip):
            return transport.post_webhook('%s://%s:%s%s' % (scheme, host, self.port, path), self.body, self.headers)

    def test_real_post_keeps_host_payload_signature_and_ignores_proxies(self):
        self.start_server()
        with patch.dict('os.environ', {'HTTP_PROXY': 'http://127.0.0.1:1', 'ALL_PROXY': 'http://127.0.0.1:1'}):
            response = self.send()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, '{}')
        self.assertEqual(self.server.bodies, [json.dumps(self.body).encode('utf-8')])
        self.assertEqual(self.server.events, [('POST', '/hook', 'receiver.example:%s' % self.port,
                                               'test-signature', self.body)])

    def test_payload_and_response_match_original_requests(self):
        self.start_server()
        self.body = {'event': 'update', 'data': {'value': '测试', 'number': 1.25,
                                               'empty': None, 'enabled': True}}
        path = '/hook/测试?text=你好'
        with patch.dict('os.environ', {'NO_PROXY': '*'}):
            original = requests.post('http://127.0.0.1:%s%s' % (self.port, path),
                                     json=self.body, headers=self.headers, timeout=30)
        updated = self.send(path=path)
        self.assertEqual(updated.status_code, original.status_code)
        self.assertEqual(updated.text, original.text)
        self.assertEqual(self.server.bodies[0], self.server.bodies[1])
        self.assertEqual(self.server.events[0][1], self.server.events[1][1])
        self.assertEqual(self.server.events[0][3], self.server.events[1][3])

    def test_international_domain_dns_and_host_match(self):
        self.start_server()
        expected_host = 'xn--fa-hia.example'
        lookups = []

        def resolve(host, port, *args, **kwargs):
            lookups.append(host)
            if host == expected_host:
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('8.8.8.8', port))]
            self.assertEqual(host, '8.8.8.8')
            return self.addresses

        with patch.object(transport.socket, 'getaddrinfo', side_effect=resolve):
            response = transport.post_webhook('http://faß.example:%s/hook' % self.port,
                                              self.body, self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(lookups, [expected_host, '8.8.8.8'])
        self.assertEqual(self.server.events[0][2], '%s:%s' % (expected_host, self.port))

    def test_redirect_is_returned_without_following(self):
        self.start_server()
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status):
                self.server.redirect_status = status
                self.server.events.clear()
                response = self.send(path='/redirect')
                self.assertEqual(response.status_code, status)
                self.assertEqual(len(self.server.events), 1)
                self.assertEqual(response.history, [])

    def start_https_server(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        cert, key = str(Path(directory.name) / 'cert.pem'), str(Path(directory.name) / 'key.pem')
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', key, '-out', cert, '-days', '1',
                        '-subj', '/CN=receiver.example',
                        '-addext', 'subjectAltName=DNS:receiver.example,DNS:localhost'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        names = []
        context.set_servername_callback(lambda sock, name, ctx: names.append(name))
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.start_server()
        return cert, directory.name, names

    def test_https_preserves_sni_and_verifies_original_hostname(self):
        cert, _, names = self.start_https_server()
        with patch.dict('os.environ', {'REQUESTS_CA_BUNDLE': '', 'CURL_CA_BUNDLE': ''}), \
                patch.object(transport.requests.certs, 'where', return_value=cert):
            self.assertEqual(self.send(scheme='https').status_code, 200)
            with self.assertRaises(SSLError):
                self.send(scheme='https', host='wrong.example')
        self.assertIn('receiver.example', names)
        self.assertEqual(len(self.server.events), 1)

    def test_https_ca_configuration_matches_original_requests(self):
        cert, directory, _ = self.start_https_server()
        ca_directory = Path(directory) / 'ca'
        ca_directory.mkdir()
        (ca_directory / 'cert.pem').write_bytes(Path(cert).read_bytes())
        subprocess.run(['openssl', 'rehash', str(ca_directory)], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        configurations = [
            {'REQUESTS_CA_BUNDLE': cert, 'CURL_CA_BUNDLE': ''},
            {'REQUESTS_CA_BUNDLE': '', 'CURL_CA_BUNDLE': cert},
            {'REQUESTS_CA_BUNDLE': cert, 'CURL_CA_BUNDLE': cert + '.missing'},
            {'REQUESTS_CA_BUNDLE': str(ca_directory), 'CURL_CA_BUNDLE': ''},
        ]
        for config in configurations:
            with self.subTest(config=config), patch.dict('os.environ', {**config, 'NO_PROXY': '*'}):
                original = requests.post('https://localhost:%s/hook' % self.port,
                                         json=self.body, headers=self.headers, timeout=30)
                updated = self.send(scheme='https')
                self.assertEqual(updated.status_code, original.status_code)
                self.assertEqual(updated.text, original.text)
                self.assertEqual(self.server.bodies[-2], self.server.bodies[-1])
                with self.assertRaises(SSLError):
                    self.send(scheme='https', host='wrong.example')


class StopWorker(BaseException):
    pass


class WebhookWorkerTest(TestCase):
    def setUp(self):
        self.worker = Webhooker.__new__(Webhooker)
        self.worker._db_session_class = Mock()
        self.worker._redis_client = Mock()
        self.worker._redis_client.get.return_value = '0'
        self.worker.job_queue = Mock()
        self.job = {'webhook_id': 42, 'created_at': None, 'url': 'http://127.0.0.1/hook',
                    'request_headers': {}, 'request_body': {'event': 'update'}}
        self.worker.job_queue.get.side_effect = [self.job, StopWorker()]
        self.worker.save_webhook_job = Mock()
        self.worker.invalidate_webhook = Mock()

    def run_worker(self):
        with self.assertRaises(StopWorker):
            self.worker.trigger_jobs()

    def test_existing_queued_private_url_is_disabled_without_http(self):
        addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 80))]
        with patch.object(transport.socket, 'getaddrinfo', return_value=addresses), \
                patch.object(transport, 'HTTPConnectionPool') as pool:
            self.run_worker()
        pool.assert_not_called()
        self.worker.invalidate_webhook.assert_called_once_with(42, self.worker._db_session_class.return_value)
        self.worker.save_webhook_job.assert_called_once()

    def test_redirect_keeps_existing_failure_threshold(self):
        with patch('dtable_events.webhook.webhook.post_webhook', return_value=SimpleNamespace(status_code=302, text='')):
            self.run_worker()
        self.worker.invalidate_webhook.assert_not_called()
        self.worker._redis_client.set.assert_called_once_with('webhook_error_42', 1, timeout=86400)
        self.worker.save_webhook_job.assert_called_once()

    def test_read_timeout_keeps_existing_failure_threshold(self):
        self.worker._redis_client.get.return_value = '4'
        with patch('dtable_events.webhook.webhook.post_webhook', side_effect=requests.exceptions.ReadTimeout()):
            self.run_worker()
        self.worker.invalidate_webhook.assert_called_once_with(42, self.worker._db_session_class.return_value)

    def test_success_clears_failure_count(self):
        with patch('dtable_events.webhook.webhook.post_webhook', return_value=SimpleNamespace(status_code=200)):
            self.run_worker()
        self.worker._redis_client.delete.assert_called_once_with('webhook_error_42')
        self.worker.save_webhook_job.assert_not_called()
