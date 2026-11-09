"""Send SeaTable source requests only to permitted, resolved addresses."""
import ipaddress
import socket
from urllib.parse import unquote, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.exceptions import ConnectTimeoutError, HTTPError, ReadTimeoutError


def _parse_url(url):
    if not isinstance(url, str) or not url or '\\' in url or any(ord(char) <= 32 or ord(char) == 127 for char in url):
        raise ValueError('Invalid SeaTable server URL.')
    parsed = urlsplit(url)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or '%' in parsed.hostname or parsed.netloc.endswith(':') or '?' in url or '#' in url
            or '..' in unquote(parsed.path).split('/')):
        raise ValueError('Invalid SeaTable server URL.')
    port = parsed.port if parsed.port is not None else (443 if parsed.scheme == 'https' else 80)
    if not 1 <= port <= 65535:
        raise ValueError('Invalid SeaTable server URL.')
    try:
        prepared = requests.Request('GET', url).prepare().url
    except requests.RequestException as error:
        raise ValueError('Invalid SeaTable server URL.') from error
    normalized = urlsplit(prepared)
    return normalized, port


def request_source(method, url, headers=None, json=None, timeout=30):
    parsed, port = _parse_url(url)
    try:
        addresses = socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError) as error:
        raise requests.ConnectionError('Unable to resolve SeaTable source.') from error
    if not addresses:
        raise requests.ConnectionError('Unable to resolve SeaTable source.')
    for family, _, _, _, sockaddr in addresses:
        if family not in (socket.AF_INET, socket.AF_INET6):
            raise ValueError('SeaTable source address is not allowed.')
        address = ipaddress.ip_address(sockaddr[0].split('%')[0])
        if isinstance(address, ipaddress.IPv6Address):
            if address.is_site_local:
                raise ValueError('SeaTable source address is not allowed.')
            address = address.ipv4_mapped or address
        if not address.is_global or address.is_multicast or address.is_reserved:
            raise ValueError('SeaTable source address is not allowed.')

    request_headers = requests.utils.default_headers()
    request_headers.update(headers or {})
    request = requests.Request(method, url, headers=request_headers, json=json).prepare()
    request.headers['Host'] = parsed.netloc
    pool_class, pool_options = HTTPConnectionPool, {}
    if parsed.scheme == 'https':
        pool_class = HTTPSConnectionPool
        pool_options = {'server_hostname': parsed.hostname, 'assert_hostname': parsed.hostname, 'cert_reqs': 'CERT_REQUIRED',
                        'ca_certs': requests.certs.where()}
    for index, (_, _, _, _, sockaddr) in enumerate(addresses):
        with pool_class(sockaddr[0], sockaddr[1], **pool_options) as pool:
            try:
                raw = pool.urlopen(request.method, request.path_url, body=request.body, headers=request.headers,
                                   timeout=timeout, retries=False, redirect=False, assert_same_host=False,
                                   preload_content=False)
            except ConnectTimeoutError as error:
                if index < len(addresses) - 1:
                    continue
                raise requests.ConnectTimeout('SeaTable source connection timed out.') from error
            except ReadTimeoutError as error:
                raise requests.ReadTimeout('SeaTable source response timed out.') from error
            except HTTPError as error:
                raise requests.ConnectionError('Unable to access SeaTable source.') from error
            response = HTTPAdapter().build_response(request, raw)
            _ = response.content
            return response
