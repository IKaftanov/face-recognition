RABBITMQ_HOST = 'host.docker.internal'  # 'localhost'

EXCHANGE_NAME = 'base'

FILES_BUS_RK = 'files_queue'

IMAGE_PREPROCESSOR_RK = 'preprocess_queue'
IMAGE_FACE_EXTRACTOR_RK = 'extractor_queue'
IMAGE_EMBEDDINGS_EXTRACTOR_RK = 'embeddings_queue'


# Model configs
DETECTOR_FRAME_BATCH_SIZE = 25
