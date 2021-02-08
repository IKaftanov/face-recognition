"""
Get frames -> MTCNN for batch of frames -> Resnet16 -> cosine
"""

import multiprocessing
import os
import pickle

import pika
import torch
from facenet_pytorch import MTCNN
from scipy import spatial

from tools.logs import get_logger
from .config import EMBEDDING_EXTRACTOR_RK, RABBITMQ_HOST, DETECTOR_FRAME_BATCH_SIZE, THRESHOLD

logger = get_logger(os.path.basename(__file__)[:-3])


def cosine_similarity(tensor_1, tensor_2):
    return 1 - spatial.distance.cosine(tensor_1, tensor_2)


def extract_faces(pil_images, model):
    faces = []
    frames = []
    for frame in pil_images:
        frames.append(frame)
        if len(frames) > DETECTOR_FRAME_BATCH_SIZE:
            faces.extend(model(frames))
            frames = []
    else:
        if frames:
            faces.extend(model(frames))
    return faces


def create_embeddings(face_tensors, model):
    result = []
    if not face_tensors:
        return result
    faces = torch.cat(face_tensors, dim=0).to(device='cuda:0' if torch.cuda.is_available() else 'cpu')
    embeddings = model(faces)
    for embedding in embeddings:
        result.append(embedding.detach().cpu().numpy())
    return result


def find_unique_embeddings(embeddings):
    if not embeddings:
        return []
    unique_embeddings = [embeddings[0]]
    for embedding in embeddings[1:]:
        for u_embedding in unique_embeddings:
            if cosine_similarity(u_embedding, embedding) > THRESHOLD:
                break
        else:
            unique_embeddings.append(embedding)
    return unique_embeddings


def on_request(ch, method, props, body):
    """

    :param ch:
    :param method:
    :param props:
    :param body: list of preprocessed images in PIL.Image format
    :return:
    """
    # define face extractor model
    mtcnn = MTCNN(keep_all=True, select_largest=False, post_process=False,
                  device='cuda:0' if torch.cuda.is_available() else 'cpu')

    # define embedding model
    resnet = torch.hub.load('pytorch/vision:v0.6.0', 'resnet18', pretrained=True)
    resnet.fc = torch.nn.Linear(in_features=512, out_features=16, bias=True)
    resnet.to(device='cuda:0' if torch.cuda.is_available() else 'cpu')
    resnet.eval()
    logger.info('Starting face extraction')
    faces = extract_faces(pickle.loads(body), mtcnn)
    logger.info('Finish face extraction')
    # filer and get tensors
    face_tensors = [face for face in faces if isinstance(face, torch.Tensor)]
    # apply resnet for embedding extraction
    logger.info('Starting embeddings creation')
    embeddings = create_embeddings(face_tensors, resnet)
    logger.info('Finish embeddings creation')
    # create unique subset
    logger.info('Starting search for unique')
    unique_embeddings = find_unique_embeddings(embeddings)
    logger.info('Finish search for unique')
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=pickle.dumps(unique_embeddings))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info('Done')


def run_instance():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=EMBEDDING_EXTRACTOR_RK)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=EMBEDDING_EXTRACTOR_RK, on_message_callback=on_request)
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
