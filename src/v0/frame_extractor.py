import multiprocessing
import os
import pickle

import cv2
import pika

from .config import EXCHANGE_NAME, FILES_BUS_RK, IMAGE_PREPROCESSOR_RK, DETECTOR_FRAME_BATCH_SIZE, RABBITMQ_HOST
from tools.logs import get_logger

logger = get_logger(os.path.basename(__file__)[:-3])


def extract_frames(channel, method, properties, body):
    logger.info(f'Received new file: <{body.decode()}> in {multiprocessing.process.current_process()} process')
    video = cv2.VideoCapture(body.decode())
    video_len = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = round(video.get(cv2.CAP_PROP_FPS))

    frames = []
    for i in range(video_len):
        video.grab()
        if i % int(fps) == 0:
            _, frame = video.retrieve()
            frames.append(frame)
            if len(frames) > DETECTOR_FRAME_BATCH_SIZE:
                channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=IMAGE_PREPROCESSOR_RK,
                                      body=pickle.dumps(frames))
                frames = []
    else:
        if frames:
            channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=IMAGE_PREPROCESSOR_RK,
                                  body=pickle.dumps(frames))
    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(f'Sent frames to "{IMAGE_PREPROCESSOR_RK}" successfully')


def consume_process() -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    # set callback for FILES_BUS_ROUTE_KEY from EXCHANGE_NAME
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')
    channel.queue_declare(queue=FILES_BUS_RK)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=FILES_BUS_RK, routing_key=FILES_BUS_RK)
    logger.info('Waiting for messages. To exit press CTRL+C')
    channel.basic_consume(queue=FILES_BUS_RK, on_message_callback=extract_frames)
    channel.start_consuming()


def run(n_jobs: int = None) -> None:
    if not n_jobs:
        n_jobs = os.cpu_count() - 1
    # spawn processes
    pool = []
    for i in range(n_jobs):
        pool.append(multiprocessing.Process(target=consume_process))
        pool[-1].start()
    # endless loop for events
    try:
        while True:
            continue
    except KeyboardInterrupt:
        logger.info('Exiting')
        for i in range(n_jobs):
            pool[i].terminate()
            pool[i].join()
        logger.info('All processes were terminated')


if __name__ == '__main__':
    run(n_jobs=2)
