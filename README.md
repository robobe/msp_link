# msp_link

Small transport-independent MultiWii Serial Protocol client for Bullet and real
flight-controller tooling. It supports TCP and serial transports, MSP v1, native
v2 and v2-over-v1 framing. `Client.request(command, payload)` is the generic
interface for every firmware command; `FlightCommands` adds typed RC, attitude,
altitude, status and firmware-identification helpers.

```python
from msp_link import Client, FlightCommands, TcpTransport

client = Client(TcpTransport("127.0.0.1", 5761), rc_hz=50).start()
flight = FlightCommands(client)
try:
    print(flight.identify())
    client.rc.update((1500, 1500, 1000, 1500, 1000, 2000))
    print(flight.read_attitude(), flight.read_altitude(), flight.read_status())
finally:
    client.rc.clear()
    client.close()
```

One worker owns the transport. Requests are bounded and synchronous while the
latest RC command is emitted independently at 50 Hz. A reconnect clears RC and
does not replay the previous command; the caller must provide a fresh command.
The library deliberately has no arming, command-age, mission or physics policy.

For hardware use `SerialTransport(device, baudrate)`. Typed measurements preserve
Betaflight's native axes and include the host monotonic receipt time because MSP
responses do not carry sensor timestamps.

`client.request_metrics(command, now_s)` returns request-send and successful-response
counts per trailing second, plus mean/maximum queue and response-turnaround delays.
It counts transport events independently of snapshot frequency and resets on
reconnect. `now_s` uses the host monotonic clock; empty timing windows report null
delays. These response rates do not measure the firmware estimator update rate.
