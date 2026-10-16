import ipaddress
import os
import socket
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.exceptions import ConnectTimeoutError, ReadTimeoutError


def resolve_webhook_url(url):
    """Resolve a webhook destination, rejecting any non-public address.
    """
    if not isinstance(url, str) or not url or '\\' in url:
        raise ValueError('Invalid webhook URL')
    if any(ord(char) <= 32 or ord(char) == 127 for char in url):
        raise ValueError('Invalid webhook URL')

    parsed = urlsplit(url)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or '%' in parsed.hostname or parsed.netloc.endswith(':')):
        raise ValueError('Invalid webhook URL')
    port = parsed.port
    if port is None:
        port = 443 if parsed.scheme == 'https' else 80
    if not 1 <= port <= 65535:
        raise ValueError('Invalid webhook port')
    prepared_url = requests.Request('POST', url).prepare().url
    host = urlsplit(prepared_url).hostname
    addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)

    if not addresses:
        raise ValueError('Webhook destination did not resolve')

    for family, _, _, _, sockaddr in addresses:
        if family not in (socket.AF_INET, socket.AF_INET6):
            raise ValueError('Invalid webhook address family')
        address = ipaddress.ip_address(sockaddr[0])
        if isinstance(address, ipaddress.IPv6Address):
            if address.is_site_local:
                raise ValueError('Webhook destination must be public')
            address = address.ipv4_mapped or address
        if not address.is_global or address.is_multicast or address.is_reserved:
            raise ValueError('Webhook destination must be public')

    return addresses


def post_webhook(url, body, headers):
    addresses = resolve_webhook_url(url)
    # Use Requests JSON serialization to preserve the signed payload.
    request_headers = requests.utils.default_headers()
    request_headers.update(headers or {})
    request = requests.Request('POST', url, json=body, headers=request_headers).prepare()
    parsed = urlsplit(request.url)
    request.headers['Host'] = parsed.netloc

    pool_class = HTTPConnectionPool
    pool_options = {}
    if parsed.scheme == 'https':
        pool_class = HTTPSConnectionPool
        ca_path = (os.environ.get('REQUESTS_CA_BUNDLE') or os.environ.get('CURL_CA_BUNDLE')
                   or requests.certs.where())
        pool_options = {'server_hostname': parsed.hostname, 'assert_hostname': parsed.hostname,
                        'cert_reqs': 'CERT_REQUIRED'}
        pool_options['ca_cert_dir' if os.path.isdir(ca_path) else 'ca_certs'] = ca_path

    for index, (_, _, _, _, sockaddr) in enumerate(addresses):
        # Connect to the validated IP without resolving the original hostname again.
        with pool_class(sockaddr[0], sockaddr[1], **pool_options) as pool:
            try:
                raw = pool.urlopen('POST', request.path_url, body=request.body, headers=request.headers,
                                   timeout=30, retries=False, redirect=False, assert_same_host=False,
                                   preload_content=False)
            except ConnectTimeoutError:
                if index == len(addresses) - 1:
                    raise
                continue
            except ReadTimeoutError as e:
                raise requests.exceptions.ReadTimeout(e) from e

            response = HTTPAdapter().build_response(request, raw)
            _ = response.content  # Read the body before closing the pool.
            return response
