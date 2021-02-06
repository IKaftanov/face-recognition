"""
    Extract faces and send them into a queue
    1. I need to send notification about starting a proccessing
    2. I need to send notification when processing is done
    ===
    I need process pictures simultaneously

"""
import pika
import multiprocessing
import os
from tools import get_logger
from face_extractor import FaceExtractor


logger = get_logger(os.path.basename(__file__)[:-3])


def callback(channel, method, properties, body):
    logger.info(f'Received in {multiprocessing.process.current_process()} message <{body.decode()}>')
    logger.info('Starting processing')
    face_extractor = FaceExtractor(detector_frame_batch_size=5)
    face_extractor.process_video(body.decode())
    logger.info('Finish')
    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(f'Acked successfully')


def consume_process(queue_name: str) -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()


def run(n_jobs: int = None, queue_name: str = 'files') -> None:
    if not n_jobs:
        n_jobs = os.cpu_count() - 1
    # spawn processes
    pool = []
    for i in range(n_jobs):
        pool.append(multiprocessing.Process(target=consume_process, args=(queue_name, )))
        pool[-1].start()
    # endless loop for events
    try:
        while True:
            continue
    except KeyboardInterrupt:
        logger.info('Exiting ...')
        for i in range(n_jobs):
            pool[i].terminate()
            pool[i].join()
        logger.info('All processes were terminated')


if __name__ == '__main__':
    run(n_jobs=3)
