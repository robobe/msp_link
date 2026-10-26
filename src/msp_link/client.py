"""Single transport owner, bounded requests and independent latest-RC streaming."""
from concurrent.futures import Future
from dataclasses import replace
from queue import Queue, Empty, Full
from threading import Event, Thread
import time

from .protocol import Parser, encode
from .rc import RcStream

REQUEST_CAPACITY = 32
RECONNECT_S = .2


class MspError(RuntimeError):
    pass


class Client:
    def __init__(self, transport, *, version='v1', rc_hz=50):
        encode(1, version=version)
        self.transport, self.version = transport, version
        self.rc = RcStream(rc_hz)
        self.queue = Queue(REQUEST_CAPACITY)
        self.stop = Event()
        self.thread = None
        self.connected = Event()
        self.generation = 0
        self.error = None

    def start(self):
        if self.thread is not None:
            raise MspError('MSP client cannot be started twice')
        self.thread = Thread(target=self._run, name='msp-link', daemon=True)
        self.thread.start()
        return self

    def request(self, command, payload=b'', *, timeout_s=.3, version=None):
        if not 0 < timeout_s <= 60:
            raise ValueError('MSP timeout must lie in (0,60] seconds')
        packet = encode(command, payload, version=version or ('v2' if command > 254 or len(payload) > 254 else self.version))
        if self.thread is None or self.stop.is_set() or not self.connected.is_set():
            raise MspError('MSP is not connected')
        future = Future()
        try:
            self.queue.put_nowait((command, packet, time.monotonic() + timeout_s, future))
        except Full as error:
            raise MspError('MSP request queue is full') from error
        try:
            return future.result(timeout=timeout_s + .1)
        except TimeoutError:
            future.cancel()
            raise

    def _run(self):
        pending = None
        try:
            while not self.stop.is_set():
                try:
                    if not self.connected.is_set():
                        self.transport.open()
                        self.rc.clear()
                        self.generation += 1
                        parser = Parser()
                        self.error = None
                        self.connected.set()
                    now = time.monotonic()
                    payload = self.rc.due(now)
                    if payload is not None:
                        self.transport.send(encode(200, payload, version=self.version))
                        self.rc.sent += 1
                        self.rc.last_sent_at = now
                    if pending is None:
                        try:
                            pending = self.queue.get_nowait()
                        except Empty:
                            pass
                        if pending is not None:
                            if pending[3].cancelled() or now >= pending[2]:
                                if not pending[3].done():
                                    pending[3].set_exception(TimeoutError('MSP request expired in queue'))
                                pending = None
                            else:
                                self.transport.send(pending[1])
                    for message in parser.feed(self.transport.receive()):
                        if pending is not None and message.direction in ('>', '!') and message.command == pending[0]:
                            if not pending[3].done():
                                if message.direction == '!':
                                    pending[3].set_exception(MspError(f'MSP command {message.command} rejected by firmware'))
                                else:
                                    pending[3].set_result(replace(message, received_at=time.monotonic()))
                            pending = None
                    if pending is not None and time.monotonic() >= pending[2]:
                        raise TimeoutError(f'MSP command {pending[0]} timed out')
                except (OSError, ValueError, TimeoutError) as error:
                    self.error = str(error)
                    self.connected.clear()
                    self.rc.clear()
                    self.transport.close()
                    if pending is not None and not pending[3].done():
                        pending[3].set_exception(MspError(str(error)))
                    pending = None
                    self._drain('MSP disconnected')
                    self.stop.wait(RECONNECT_S)
        finally:
            self.connected.clear()
            self.rc.clear()
            self.transport.close()
            if pending is not None and not pending[3].done():
                pending[3].set_exception(MspError('MSP closed'))
            self._drain('MSP closed')

    def _drain(self, reason):
        while True:
            try:
                _, _, _, future = self.queue.get_nowait()
            except Empty:
                return
            if not future.done():
                future.set_exception(MspError(reason))

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join(2)
            if self.thread.is_alive():
                raise MspError('MSP worker did not stop')
