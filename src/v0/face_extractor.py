import functools
import multiprocessing
import os
import pickle

import pika
import torch
from facenet_pytorch import MTCNN

from .config import EXCHANGE_NAME, IMAGE_FACE_EXTRACTOR_RK, IMAGE_EMBEDDINGS_EXTRACTOR_RK, RABBITMQ_HOST
from tools.logs import get_logger

logger = get_logger(os.path.basename(__file__)[:-3])


def callback(channel, method, properties, body, nn):
    logger.info(f'Received frames in {multiprocessing.process.current_process()} process')
    faces = [face for face in nn(pickle.loads(body)) if isinstance(face, torch.Tensor)]
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=IMAGE_EMBEDDINGS_EXTRACTOR_RK,
                          body=pickle.dumps(faces))
    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(f'Sent {len(faces)} to "{IMAGE_EMBEDDINGS_EXTRACTOR_RK}" queue')


def consume_process() -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')
    channel.queue_declare(queue=IMAGE_FACE_EXTRACTOR_RK)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=IMAGE_FACE_EXTRACTOR_RK, routing_key=IMAGE_FACE_EXTRACTOR_RK)
    # define a nn model
    mtcnn = MTCNN(keep_all=True, select_largest=False, post_process=False,
                  device='cuda:0' if torch.cuda.is_available() else 'cpu')
    callback_with_nn = functools.partial(callback, nn=mtcnn)
    logger.info('Waiting for messages. To exit press CTRL+C')
    channel.basic_consume(queue=IMAGE_FACE_EXTRACTOR_RK, on_message_callback=callback_with_nn)
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
    run(n_jobs=3)
