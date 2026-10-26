"""Latest-value RC schedule. No mission policy or command-expiry failsafe."""
from math import isfinite
import struct
from threading import Lock


def pack_rc(channels):
    values = tuple(channels)
    if not 4 <= len(values) <= 18 or any(type(v) is not int or not 1000 <= v <= 2000 for v in values):
        raise ValueError('RC requires 4..18 integer channels in 1000..2000')
    return struct.pack('<' + 'H' * len(values), *values)


class RcStream:
    def __init__(self, hz=50):
        if isinstance(hz, bool) or not isfinite(hz) or not 1 <= hz <= 200:
            raise ValueError('RC stream rate must be in 1..200 Hz')
        self.period = 1 / hz
        self.lock = Lock()
        self.payload = None
        self.next_send = 0.
        self.sent = 0
        self.last_sent_at = None

    def update(self, channels):
        payload = pack_rc(channels)
        with self.lock:
            self.payload = payload

    def clear(self):
        with self.lock:
            self.payload = None
            self.next_send = 0.

    def due(self, now):
        with self.lock:
            if self.payload is None or now < self.next_send:
                return None
            self.next_send = now + self.period
            return self.payload
