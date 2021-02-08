"""
Load video -> extrace frames -> convert to grayscale
"""

import multiprocessing
import os
import pickle

import cv2
import pika
from PIL import Image

from tools.logs import get_logger
from .config import IMAGE_PREPROCESSOR_RK, RABBITMQ_HOST

logger = get_logger(os.path.basename(__file__)[:-3])


def extract_frames(video):
    video_len = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = round(video.get(cv2.CAP_PROP_FPS))
    frames = []
    for i in range(video_len):
        video.grab()
        if i % int(fps) == 0:
            _, frame = video.retrieve()
            frames.append(frame)
    logger.info('Frames collected')
    return frames


def convert_to_grayscale(frames):
    logger.info('Converting to grayscale')
    processed_frames = []
    for frame in frames:
        processed_frames.append(Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)))
    return processed_frames


def on_request(ch, method, props, body):
    path_to_video = pickle.loads(body)
    logger.info(f'I got {path_to_video}')
    video = cv2.VideoCapture(path_to_video)
    response = convert_to_grayscale(extract_frames(video))
    logger.info('Responding')
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=pickle.dumps(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info('Done')


def run_instance():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=IMAGE_PREPROCESSOR_RK)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=IMAGE_PREPROCESSOR_RK, on_message_callback=on_request)
    logger.info(f'{multiprocessing.process.current_process()} is waiting for messages')
    channel.start_consuming()


def run_instances(n_jobs=2):
    pool = []
    for i in range(n_jobs):
        pool.append(multiprocessing.Process(target=run_instance))
        pool[-1].start()

    try:
        while True:
            continue
    except KeyboardInterrupt:
        logger.info('Stopping service')
        for i in range(n_jobs):
            pool[i].terminate()
            pool[i].join()
        logger.info('Service has been stopped')


if __name__ == '__main__':
    run_instances(2)