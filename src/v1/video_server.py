import websockets
import asyncio
import os
from tools.logs import get_logger

logger = get_logger(os.path.basename(__file__)[:-3])


class Server:
    clients = set()

    def __init__(self):
        pass

    async def register(self, ws: websockets.WebSocketServerProtocol):
        self.clients.add(ws)
        logger.info(f'{ws.remote_address} connected')

    async def unregister(self, ws: websockets.WebSocketServerProtocol):
        self.clients.remove(ws)
        logger.info(f'{ws.remote_address} disconnected')

    async def send_to_client(self, message: str):
        if self.clients:
            await asyncio.wait([client.send(message) for client in self.clients])

    async def ws_handler(self, ws: websockets.WebSocketServerProtocol, uri: str):
        await self.register(ws)
        try:
            await self.distribute(ws)
        finally:
            await self.unregister(ws)

    async def distribute(self, ws: websockets.WebSocketServerProtocol):
        async for message in ws:
            await self.send_to_client(message)


server = Server()
start = websockets.serve(server.ws_handler, 'localhost', 4000)
loop = asyncio.get_event_loop()
loop.run_until_complete(start)
loop.run_forever()
