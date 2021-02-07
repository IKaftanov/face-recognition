import functools
import multiprocessing
import os
import pickle

import pika
import torch
from scipy import spatial

from .config import EXCHANGE_NAME, IMAGE_EMBEDDINGS_EXTRACTOR_RK, RABBITMQ_HOST
from tools.logs import get_logger

logger = get_logger(os.path.basename(__file__)[:-3])


def cosine_similarity(tensor_1, tensor_2):
    return 1 - spatial.distance.cosine(tensor_1, tensor_2)


def callback(channel, method, properties, body, nn, unique_embeddings):
    logger.info(f'Received faces in {multiprocessing.process.current_process()} process')
    body = torch.cat(pickle.loads(body), dim=0).to('cuda:0' if torch.cuda.is_available() else 'cpu')
    embeddings_tensor = nn(body)
    embeddings = []
    for embedding in embeddings_tensor:
        embeddings.append(embedding.detach().cpu().numpy())
    # check for unique
    for embedding in embeddings:
        for unique_embedding in unique_embeddings:
            if cosine_similarity(unique_embedding, embedding) > 0.99:  # change to constant
                break
        else:
            unique_embeddings.append(embedding)
    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(f'Total number of unique faces is {len(unique_embeddings)}')


def consume_process(unique_embeddings) -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')
    channel.queue_declare(queue=IMAGE_EMBEDDINGS_EXTRACTOR_RK)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=IMAGE_EMBEDDINGS_EXTRACTOR_RK,
                       routing_key=IMAGE_EMBEDDINGS_EXTRACTOR_RK)

    resnet = torch.hub.load('pytorch/vision:v0.6.0', 'resnet18', pretrained=True)
    resnet.fc = torch.nn.Linear(in_features=512, out_features=16, bias=True)
    resnet.to(device='cuda:0' if torch.cuda.is_available() else 'cpu')
    resnet.eval()
    callback_with_nn = functools.partial(callback, nn=resnet, unique_embeddings=unique_embeddings)
    logger.info('Waiting for messages. To exit press CTRL+C')
    channel.basic_consume(queue=IMAGE_EMBEDDINGS_EXTRACTOR_RK, on_message_callback=callback_with_nn)
    channel.start_consuming()


def run(n_jobs=None):
    manager = multiprocessing.Manager()
    unique_embeddings = manager.list([])

    n_jobs = n_jobs or 3
    # spawn processes
    pool = []
    for i in range(n_jobs):
        pool.append(multiprocessing.Process(target=consume_process, args=(unique_embeddings,)))
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
    run()
