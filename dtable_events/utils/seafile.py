import ipaddress
import os
import socket
from urllib.parse import unquote, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.exceptions import ConnectTimeoutError, ReadTimeoutError


def check_seafile_url(url, base=True):
    """Validate a Seafile URL and all resolved IPs, returning socket addresses.

    Base URLs cannot contain queries; full request/upload URLs use base=False.
    Raise ValueError for invalid or disallowed destinations. Private networks
    remain allowed for internal Seafile installations.
    """
    if not isinstance(url, str) or not url or '\\' in url:
        raise ValueError('Invalid Seafile URL')
    if any(ord(char) <= 32 or ord(char) == 127 for char in url):
        raise ValueError('Invalid Seafile URL')
    parsed = urlsplit(url)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or '%' in parsed.hostname or parsed.netloc.endswith(':')
            or '#' in url or '..' in unquote(parsed.path).split('/')):
        raise ValueError('Invalid Seafile URL')
    if base and '?' in url:
        raise ValueError('Seafile base URL must not contain a query')
    port = parsed.port
    if port is None:
        port = 443 if parsed.scheme == 'https' else 80
    if not 1 <= port <= 65535:
        raise ValueError('Invalid Seafile port')
    # Resolve the same normalized hostname Requests will use, including IDNA.
    host = urlsplit(requests.Request('GET', url).prepare().url).hostname
    try:
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError) as error:
        raise ValueError('Seafile destination did not resolve') from error
    if not addresses:
        raise ValueError('Seafile destination did not resolve')
    for family, _, _, _, sockaddr in addresses:
        if family not in (socket.AF_INET, socket.AF_INET6):
            raise ValueError('Invalid Seafile address family')
        address = ipaddress.ip_address(sockaddr[0].split('%')[0])
        if isinstance(address, ipaddress.IPv6Address):
            address = address.ipv4_mapped or address
        # Preserve support for private-network Seafile installations.
        if (address.is_loopback or address.is_link_local or address.is_unspecified
                or address.is_multicast or address.is_reserved):
            raise ValueError('Seafile destination is not allowed')
    return addresses


def request_seafile(method, url, headers=None, params=None, files=None, data=None, timeout=None):
    """Send to validated IPs while preserving TLS identity and refusing redirects."""
    addresses = check_seafile_url(url, base=False)
    request_headers = requests.utils.default_headers()
    request_headers.update(headers or {})
    request = requests.Request(method, url, headers=request_headers, params=params,
                               files=files, data=data).prepare()
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
        # Connect only to validated IPs, never resolving the URL hostname again.
        with pool_class(sockaddr[0], sockaddr[1], **pool_options) as pool:
            try:
                raw = pool.urlopen(request.method, request.path_url, body=request.body,
                                   headers=request.headers, timeout=timeout, retries=False,
                                   redirect=False, assert_same_host=False, preload_content=False)
            except ConnectTimeoutError:
                if index == len(addresses) - 1:
                    raise
                continue
            except ReadTimeoutError as error:
                raise requests.exceptions.ReadTimeout(error) from error
            response = HTTPAdapter().build_response(request, raw)
            _ = response.content
            if 300 <= response.status_code < 400:
                raise requests.exceptions.HTTPError('Seafile redirects are not allowed', response=response)
            return response
