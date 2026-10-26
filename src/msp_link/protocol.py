"""Incremental bounded MSP framing, independent of transport and firmware."""
from dataclasses import dataclass
import struct
from functools import reduce
from operator import xor

MAX_PAYLOAD = 65535
MAX_BUFFER = 2 * (MAX_PAYLOAD + 16)


@dataclass(frozen=True)
class Message:
    command: int
    payload: bytes
    direction: str = '>'
    version: str = 'v1'
    received_at: float = 0.


def crc8(data):
    crc = 0
    for value in data:
        crc ^= value
        for _ in range(8):
            crc = ((crc << 1) ^ (0xD5 if crc & 0x80 else 0)) & 255
    return crc


def encode(command, payload=b'', *, version='v1', direction='<'):
    if type(command) is not int or not 0 <= command <= 65535:
        raise ValueError('MSP command must be an integer in 0..65535')
    if not isinstance(payload, bytes) or len(payload) > MAX_PAYLOAD or direction not in ('<', '>', '!'):
        raise ValueError('Invalid MSP payload or direction')
    if version in ('v2', 'v2-in-v1'):
        body = struct.pack('<BHH', 0, command, len(payload)) + payload
        body += bytes([crc8(body)])
        if version == 'v2':
            return b'$X' + direction.encode() + body
        return encode(255, body, direction=direction)
    if version != 'v1' or command > 255 or len(payload) > 254:
        raise ValueError('MSP v1 requires an 8-bit command and payload <=254 bytes')
    body = bytes([len(payload), command]) + payload
    return b'$M' + direction.encode() + body + bytes([reduce(xor, body, 0)])


class Parser:
    def __init__(self):
        self.buffer = bytearray()

    def feed(self, data):
        self.buffer.extend(data)
        if len(self.buffer) > MAX_BUFFER:
            self.buffer.clear()
            raise ValueError('MSP receive buffer exceeded its bound')
        messages = []
        while len(self.buffer) >= 3:
            if self.buffer[:2] not in (b'$M', b'$X') or self.buffer[2] not in b'<>!':
                del self.buffer[0]
                continue
            v2 = self.buffer[1] == ord('X')
            header = 8 if v2 else 5
            if len(self.buffer) < header:
                break
            length = struct.unpack_from('<H', self.buffer, 6)[0] if v2 else self.buffer[3]
            total = header + length + 1
            if len(self.buffer) < total:
                break
            packet = bytes(self.buffer[:total])
            valid = crc8(packet[3:-1]) == packet[-1] if v2 else reduce(xor, packet[3:], 0) == 0
            if not valid:
                del self.buffer[0]
                continue
            del self.buffer[:total]
            command = struct.unpack_from('<H', packet, 4)[0] if v2 else packet[4]
            payload = packet[header:-1]
            version = 'v2' if v2 else 'v1'
            if not v2 and command == 255:
                if len(payload) < 6 or len(payload) != struct.unpack_from('<H', payload, 3)[0] + 6 or crc8(payload[:-1]) != payload[-1]:
                    continue
                command = struct.unpack_from('<H', payload, 1)[0]
                payload = payload[5:-1]
                version = 'v2-in-v1'
            messages.append(Message(command, payload, chr(packet[2]), version))
        return messages
