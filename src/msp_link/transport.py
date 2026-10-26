"""TCP and UART adapters with short bounded reads for the single I/O owner."""
import socket

READ_TIMEOUT_S = .002
CONNECT_TIMEOUT_S = .3


class TcpTransport:
    def __init__(self, host='127.0.0.1', port=5761):
        self.host, self.port = host, port
        self.socket = None

    def open(self):
        self.socket = socket.create_connection((self.host, self.port), timeout=CONNECT_TIMEOUT_S)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket.settimeout(READ_TIMEOUT_S)

    def send(self, data):
        self.socket.sendall(data)

    def receive(self):
        try:
            data = self.socket.recv(65536)
        except socket.timeout:
            return b''
        if not data:
            raise ConnectionError('MSP peer closed the connection')
        return data

    def close(self):
        if self.socket is not None:
            self.socket.close()
            self.socket = None


class SerialTransport:
    def __init__(self, device, baudrate=115200):
        self.device, self.baudrate = device, baudrate
        self.serial = None

    def open(self):
        import serial
        self.serial = serial.Serial(self.device, self.baudrate, timeout=READ_TIMEOUT_S, write_timeout=.05)

    def send(self, data):
        if self.serial.write(data) != len(data):
            raise ConnectionError('Incomplete MSP serial write')

    def receive(self):
        return self.serial.read(max(1, min(self.serial.in_waiting, 65536)))

    def close(self):
        if self.serial is not None:
            self.serial.close()
            self.serial = None
