import multiprocessing
import os
import pickle

import cv2
import pika
from PIL import Image

from .config import EXCHANGE_NAME, IMAGE_PREPROCESSOR_RK, IMAGE_FACE_EXTRACTOR_RK, RABBITMQ_HOST
from tools.logs import get_logger

logger = get_logger(os.path.basename(__file__)[:-3])


def get_frames(channel, method, properties, body):
    logger.info(f'Received data in {multiprocessing.process.current_process()} process>')
    processed_frames = []
    for frame in pickle.loads(body):
        processed_frames.append(Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)))
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=IMAGE_FACE_EXTRACTOR_RK,
                          body=pickle.dumps(processed_frames))
    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(f'Sent grayscale frames to "{IMAGE_FACE_EXTRACTOR_RK}" queue')


def listen_videos() -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')
    channel.queue_declare(queue=IMAGE_PREPROCESSOR_RK)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=IMAGE_PREPROCESSOR_RK, routing_key=IMAGE_PREPROCESSOR_RK)
    logger.info('Waiting for messages. To exit press CTRL+C')
    channel.basic_consume(queue=IMAGE_PREPROCESSOR_RK, on_message_callback=get_frames)
    channel.start_consuming()


def run(n_jobs: int = None) -> None:
    if not n_jobs:
        n_jobs = os.cpu_count() - 1
    # spawn processes
    pool = []
    for i in range(n_jobs):
        pool.append(multiprocessing.Process(target=listen_videos))
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
    run(n_jobs=1)
