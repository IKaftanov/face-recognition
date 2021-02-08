import asyncio
import concurrent.futures
import functools
import os

import websockets
from scipy import spatial

from clients.queue_rpc_client import RpcClient
from config import RABBITMQ_HOST, IMAGE_PREPROCESSOR_RK, EMBEDDING_EXTRACTOR_RK, THRESHOLD
from tools.logs import get_logger

logger = get_logger(os.path.basename(__file__)[:-3])


def cosine_similarity(tensor_1, tensor_2):
    return 1 - spatial.distance.cosine(tensor_1, tensor_2)


def decode_input_and_send_to_queue(videos: str):
    # run rpc queue
    videos = videos.split(',')
    # I should move it higher
    preprocessor_client = RpcClient(host=RABBITMQ_HOST, queue_name=IMAGE_PREPROCESSOR_RK,
                                    client_name='preprocessor_client')
    face_counter_client = RpcClient(host=RABBITMQ_HOST, queue_name=EMBEDDING_EXTRACTOR_RK,
                                    client_name='face_counter_client')
    unique_faces = []
    for video in videos:
        path_to_video = os.path.abspath(video)
        logger.info(f'Pre-Processing {path_to_video}')
        result = preprocessor_client.call(path_to_video)
        logger.info(f'Extracting faces, creating embeddings, counting unique faces')
        unique_faces.extend(face_counter_client.call(result))

    logger.info(f'Searching for unique')
    result = [unique_faces[0]]
    for face in unique_faces:
        for unique_face in result:
            if cosine_similarity(face, unique_face) > THRESHOLD:
                break
        else:
            result.append(face)
    logger.info(f'I found {len(result)} unique embeddings')
    return len(result)


class Server:
    clients = set()

    def __init__(self):
        pass

    async def register(self, ws: websockets.WebSocketServerProtocol):
        self.clients.add(ws)
        logger.info(f'{ws.remote_address} connected')

    async def process_video(self, ws: websockets.WebSocketServerProtocol):
        loop = asyncio.get_event_loop()
        videos = await ws.recv()  # list of paths to videos
        await ws.send('We are starting to process your videos. Please wait')
        with concurrent.futures.ProcessPoolExecutor() as pool:
            result = await loop.run_in_executor(pool,
                                                functools.partial(decode_input_and_send_to_queue, videos))
        await ws.send(f'We found {result} unique faces')

    async def unregister(self, ws: websockets.WebSocketServerProtocol):
        self.clients.remove(ws)
        logger.info(f'{ws.remote_address} disconnected')

    async def send_to_client(self, message: str):
        if self.clients:
            await asyncio.wait([client.send(message) for client in self.clients])

    async def ws_handler(self, ws: websockets.WebSocketServerProtocol, uri: str):
        await self.register(ws)
        try:
            await self.process_video(ws)
        finally:
            await self.unregister(ws)


if __name__ == '__main__':
    server = Server()
    start = websockets.serve(server.ws_handler, 'localhost', 4000)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start)
    loop.run_forever()
