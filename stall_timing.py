"""PROTOTYPE. Where is the 40 ms? Time each recv() segment of a keep-alive GET /health."""
import socket, sys, time
host, port = "127.0.0.1", int(sys.argv[1])
s = socket.create_connection((host, port))
if len(sys.argv) > 2 and sys.argv[2] == "nodelay":
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
req = b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n"
for i in range(4):
    t0 = time.perf_counter()
    s.sendall(req)
    segs = []
    data = b""
    while True:
        chunk = s.recv(65536)
        segs.append((round((time.perf_counter() - t0) * 1000, 1), len(chunk)))
        data += chunk
        head, _, rest = data.partition(b"\r\n\r\n")
        if _ and len(rest) >= int([l for l in head.lower().split(b"\r\n") if l.startswith(b"content-length")][0].split(b":")[1]):
            break
    print(f"req{i}: segments (ms_since_send, bytes) = {segs}")
