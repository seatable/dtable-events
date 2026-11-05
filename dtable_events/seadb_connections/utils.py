"""Shared sync intervals and errors."""
SYNC_INTERVAL = 600


class UpstreamUnavailable(Exception):
    pass
