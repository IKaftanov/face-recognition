# WS client example
import asyncio

import websockets


async def baseline():
    uri = "ws://localhost:4000"
    async with websockets.connect(uri) as websocket:
        await asyncio.sleep(5)
        # blocking code

        await websocket.send('videos/aapnvogymq.mp4,videos/abarnvbtwb.mp4,videos/abofeumbvv.mp4')

        process_start_confirmation = await websocket.recv()
        print(process_start_confirmation)
        result = await websocket.recv()
        print(result)


asyncio.get_event_loop().run_until_complete(baseline())
