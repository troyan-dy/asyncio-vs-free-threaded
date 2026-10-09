"""PROTOTYPE. TCP_NODELAY on accepted sockets: start_server(host) vs create_server(sock=) vs start_server(sock=)."""
import asyncio, socket, sys

def listening():
    s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("127.0.0.1", 0)); s.listen(8); return s

class P(asyncio.Protocol):
    def __init__(self, box): self.box = box
    def connection_made(self, tr):
        self.box["nodelay"] = tr.get_extra_info("socket").getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY); tr.close()

async def probe(name, make_server):
    box = {}
    loop = asyncio.get_running_loop()
    srv, port = await make_server(loop, box)
    r, w = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.05); w.close(); srv.close()
    print(f"  {name:32} accepted-socket TCP_NODELAY = {box.get('nodelay')}")

async def main():
    async def via_start_server(loop, box):
        async def h(r, w): box["nodelay"] = w.get_extra_info("socket").getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY); w.close()
        srv = await asyncio.start_server(h, "127.0.0.1", 0); return srv, srv.sockets[0].getsockname()[1]
    async def via_create_server_sock(loop, box):
        ls = listening(); srv = await loop.create_server(lambda: P(box), sock=ls); return srv, ls.getsockname()[1]
    async def via_create_server_host(loop, box):
        srv = await loop.create_server(lambda: P(box), "127.0.0.1", 0); return srv, srv.sockets[0].getsockname()[1]
    await probe("start_server(host)", via_start_server)
    await probe("create_server(host)", via_create_server_host)
    await probe("create_server(sock=)", via_create_server_sock)

print("asyncio SelectorEventLoop", sys.version.split()[0]); asyncio.run(main())
import uvloop; print("uvloop"); uvloop.run(main())
