"""Typed flight helpers. Units are SI, axes retain firmware convention."""
from dataclasses import dataclass
from math import radians
import struct

from .rc import pack_rc

API_VERSION = 1
FC_VARIANT = 2
FC_VERSION = 3
STATUS = 101
ATTITUDE = 108
ALTITUDE = 109
SET_RAW_RC = 200


@dataclass(frozen=True)
class Attitude:
    roll_rad: float
    pitch_rad: float
    yaw_rad: float
    received_at: float


@dataclass(frozen=True)
class Altitude:
    altitude_m: float
    vertical_speed_m_s: float
    received_at: float


@dataclass(frozen=True)
class Status:
    mode_flags: int
    arming_disabled_flags: int
    received_at: float

    @property
    def armed(self):
        return bool(self.mode_flags & 1)


class FlightCommands:
    def __init__(self, client):
        self.client = client

    def identify(self):
        return {name: self.client.request(command).payload for name, command in
                (('api', API_VERSION), ('variant', FC_VARIANT), ('version', FC_VERSION))}

    def read_attitude(self):
        message = self.client.request(ATTITUDE)
        if len(message.payload) != 6:
            raise ValueError('Invalid MSP_ATTITUDE length')
        roll, pitch, yaw = struct.unpack('<3h', message.payload)
        return Attitude(radians(roll / 10), radians(pitch / 10), radians(yaw), message.received_at)

    def read_altitude(self):
        message = self.client.request(ALTITUDE)
        if len(message.payload) < 6:
            raise ValueError('Invalid MSP_ALTITUDE length')
        altitude, speed = struct.unpack_from('<ih', message.payload)
        return Altitude(altitude / 100, speed / 100, message.received_at)

    def read_status(self):
        message = self.client.request(STATUS)
        if len(message.payload) < 11:
            raise ValueError('Invalid MSP_STATUS length')
        modes = struct.unpack_from('<I', message.payload, 6)[0]
        flags = 0
        if len(message.payload) >= 21:
            extra = message.payload[15]
            if len(message.payload) < 21 + extra:
                raise ValueError('Truncated MSP arming flags')
            flags = struct.unpack_from('<I', message.payload, 17 + extra)[0]
        return Status(modes, flags, message.received_at)

    def send_rc(self, channels):
        return self.client.request(SET_RAW_RC, pack_rc(channels))
