"""PROTOTYPE. Wire format shared by the fake backends (backends.py, echo_server.py) and the service."""

import struct

DB_PORT_BASE = 9000  # "DB" echo servers listen on DB_PORT_BASE .. DB_PORT_BASE + DB_PORTS - 1
DB_PORTS = 4
DB_MSG_SIZE = 64  # bytes: fixed-size query, echoed back as the answer
STREAM_PORT = 9100
STREAM_MSG = struct.Struct("<qid")  # seq, symbol, value (padded to STREAM_MSG_SIZE bytes)
STREAM_MSG_SIZE = 64
