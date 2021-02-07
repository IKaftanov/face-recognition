"""
    Create a demon for files handling
"""

import os
import time

import pika
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .config import EXCHANGE_NAME, FILES_BUS_RK, RABBITMQ_HOST
from tools.logs import get_logger

logger = get_logger(os.path.basename(__file__)[:-3])


class FileLoadedEvent(FileSystemEventHandler):
    def __init__(self, channel,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.channel = channel

    def on_created(self, event):
        path_to_file = os.path.abspath(event.src_path)
        logger.info(f'find new file: {path_to_file}')
        self.channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=FILES_BUS_RK,
                                   body=path_to_file)


def run(path_to_dir='v0/videos'):
    pwd = os.path.abspath(os.path.curdir)
    logger.info(f'current directory = {pwd}')
    observer = Observer()

    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

    event_handler = FileLoadedEvent(channel=channel)
    observer.schedule(event_handler, os.path.join(pwd, path_to_dir), recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    connection.close()


if __name__ == '__main__':
    run()
