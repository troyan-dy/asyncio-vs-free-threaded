"""PROTOTYPE. Where is the 40 ms? Time each recv() segment of a keep-alive GET /health.

    python stall_timing.py PORT [nodelay]
"""

import socket
import sys
import time

HOST = "127.0.0.1"
REQUEST = b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n"
REQUESTS = 4
RECV_CHUNK = 65536
HEAD_END = b"\r\n\r\n"
MS_PER_S = 1000

Segment = tuple[float, int]  # ms since the request was sent, bytes received


def time_segments(sock: socket.socket) -> list[Segment]:
    """Send REQUEST and time every recv() until the whole response is in."""
    started = time.perf_counter()
    sock.sendall(REQUEST)
    segments = []
    received = b""
    while True:
        chunk = sock.recv(RECV_CHUNK)
        elapsed_ms = round((time.perf_counter() - started) * MS_PER_S, 1)
        segments.append((elapsed_ms, len(chunk)))
        received += chunk
        if _is_complete(received):
            return segments


def _is_complete(received: bytes) -> bool:
    head, separator, body = received.partition(HEAD_END)
    return bool(separator) and len(body) >= _content_length(head)


def _content_length(head: bytes) -> int:
    headers = head.lower().split(b"\r\n")
    lines = [line for line in headers if line.startswith(b"content-length")]
    return int(lines[0].split(b":")[1])


def main() -> None:
    """Send REQUESTS requests on one connection (with TCP_NODELAY if asked) and print the segments."""
    sock = socket.create_connection((HOST, int(sys.argv[1])))
    if sys.argv[2:3] == ["nodelay"]:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    for index in range(REQUESTS):
        segments = time_segments(sock)
        sys.stdout.write(f"req{index}: segments (ms_since_send, bytes) = {segments}\n")


if __name__ == "__main__":
    main()
