import struct
from threading import Lock
import time
import os

import pytest

from msp_link import Client, Parser, SerialTransport, encode
from msp_link.commands import FlightCommands
from msp_link.protocol import Message
from msp_link.rc import RcStream, pack_rc


@pytest.mark.parametrize('version,command,payload', [
    ('v1', 108, b'abc'), ('v2', 0x301, bytes(range(255))),
    ('v2-in-v1', 0x301, b'wrapped'),
])
def test_round_trip_fragmented_and_recovers_from_noise(version, command, payload):
    packet = encode(command, payload, version=version, direction='>')
    parser = Parser()
    messages = parser.feed(b'noise' + packet[:4]) + parser.feed(packet[4:])
    assert messages == [Message(command, payload, '>', version)]


def test_bad_checksum_is_discarded_without_losing_next_packet():
    bad = bytearray(encode(108, b'bad', direction='>'))
    bad[-1] ^= 1
    assert Parser().feed(bytes(bad) + encode(109, b'ok', direction='>')) == [Message(109, b'ok')]


def test_rc_validation_and_schedule_repeats_latest_value():
    stream = RcStream(50)
    stream.update((1000, 1500, 2000, 1500))
    assert stream.due(1.) == pack_rc((1000, 1500, 2000, 1500))
    assert stream.due(1.019) is None
    assert stream.due(1.020) is not None
    stream.clear()
    assert stream.due(2.) is None
    with pytest.raises(ValueError):
        pack_rc((999,) * 4)


class StubClient:
    def __init__(self, replies):
        self.replies = iter(replies)

    def request(self, command, payload=b''):
        return Message(command, next(self.replies), received_at=12.5)


def test_typed_flight_decoding_uses_si_units_and_receipt_time():
    commands = FlightCommands(StubClient([
        struct.pack('<3h', 100, -200, 90),
        struct.pack('<ih', 1234, -56),
        b'\0' * 6 + struct.pack('<I', 1) + b'\0' * 5 + b'\0' + b'\0' + struct.pack('<I', 8),
    ]))
    attitude = commands.read_attitude()
    altitude = commands.read_altitude()
    status = commands.read_status()
    assert attitude.roll_rad == pytest.approx(0.174532925) and attitude.pitch_rad == pytest.approx(-0.34906585)
    assert attitude.yaw_rad == pytest.approx(1.570796327) and attitude.received_at == 12.5
    assert (altitude.altitude_m, altitude.vertical_speed_m_s) == (12.34, -.56)
    assert status.armed and status.arming_disabled_flags == 8


class LoopbackTransport:
    def __init__(self):
        self.lock = Lock()
        self.responses = []
        self.sent = []
        self.disconnect = False

    def open(self): pass

    def send(self, packet):
        message = Parser().feed(packet)[0]
        with self.lock:
            self.sent.append(message)
            if message.command != 200:
                self.responses.append(encode(message.command, b'reply', direction='>'))

    def receive(self):
        with self.lock:
            if self.disconnect:
                self.disconnect = False
                raise ConnectionError('injected disconnect')
            return self.responses.pop(0) if self.responses else b''

    def close(self): pass


def wait_for(predicate, timeout=.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.002)
    raise AssertionError('condition timed out')


def test_client_services_requests_while_repeating_rc_and_does_not_replay_after_reconnect():
    transport = LoopbackTransport()
    client = Client(transport, rc_hz=50).start()
    try:
        assert client.connected.wait(.2)
        assert client.request(42).payload == b'reply'
        client.rc.update((1500, 1500, 1000, 1500))
        wait_for(lambda: sum(message.command == 200 for message in transport.sent) >= 3)
        with transport.lock:
            transport.disconnect = True
        generation = client.generation
        wait_for(lambda: client.generation > generation)
        after_reconnect = sum(message.command == 200 for message in transport.sent)
        time.sleep(.06)
        assert sum(message.command == 200 for message in transport.sent) == after_reconnect
        client.rc.update((1500, 1500, 1000, 1500))
        wait_for(lambda: sum(message.command == 200 for message in transport.sent) > after_reconnect)
    finally:
        client.close()


def test_serial_transport_uses_a_real_pseudo_terminal():
    master, slave = os.openpty()
    transport = SerialTransport(os.ttyname(slave))
    try:
        transport.open()
        packet = encode(108)
        transport.send(packet)
        assert os.read(master, len(packet)) == packet
        response = encode(108, b'ok', direction='>')
        os.write(master, response)
        deadline = time.monotonic() + .2
        received = b''
        while len(received) < len(response) and time.monotonic() < deadline:
            received += transport.receive()
        assert received == response
    finally:
        transport.close()
        os.close(master)
        os.close(slave)


def test_request_metrics_count_responses_before_snapshot_sampling():
    client = Client(LoopbackTransport()).start()
    try:
        assert client.connected.wait(.2)
        for _ in range(3):
            client.request(108)
        client.request(109)
        now = time.monotonic()
        metrics = client.request_metrics(108, now)
        assert metrics['requests_hz'] == metrics['responses_hz'] == 3
        assert metrics['queue_delay_mean_s'] >= 0
        assert metrics['response_turnaround_mean_s'] >= 0
        assert client.request_metrics(108, now) == metrics
        assert client.request_metrics(108, now + 2)['responses_hz'] == 0
    finally:
        client.close()
