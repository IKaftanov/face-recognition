"""
    Create a demon for files handling
"""

import os
import time
import pika
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from tools import get_logger

logger = get_logger(os.path.basename(__file__)[:-3])


class FileLoadedEvent(FileSystemEventHandler):
    def __init__(self, channel, routing_key,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.channel = channel
        self.routing_key = routing_key

    def on_created(self, event):
        path_to_file = os.path.abspath(event.src_path)
        logger.info(f'find new file: {path_to_file}')
        self.channel.basic_publish(exchange='', routing_key=self.routing_key,
                                   body=path_to_file)


class VideoLoader:
    """
    I want to load videos from folder concurrently and then send them into a queue

    """

    def __init__(self, path_to_dir, queue_name):
        self.path_to_dir = path_to_dir
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)  # durable=True if needed

    def run(self):
        """
        endless loop
        :return:
        """
        observer = Observer()
        event_handler = FileLoadedEvent(channel=self.channel, routing_key=self.queue_name)
        observer.schedule(event_handler, self.path_to_dir, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
        self.connection.close()


if __name__ == '__main__':
    video_loader = VideoLoader(path_to_dir='../videos',
                               queue_name='files')
    video_loader.run()
