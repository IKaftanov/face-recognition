import os
from multiprocessing import Process
from tools.logs import get_logger

from . import embedding_extractor
from . import face_extractor
from . import frame_extractor
from . import frame_preprocessor
from . import video_puller_dir


logger = get_logger(os.path.basename(__file__)[:-3])


if __name__ == '__main__':
    video_loader_process = Process(target=video_puller_dir.run)
    video_loader_process.start()

    frame_extractor_process = Process(target=frame_extractor.run, args=(2,))
    frame_extractor_process.start()

    frame_preprocessor_process = Process(target=frame_preprocessor.run, args=(2,))
    frame_preprocessor_process.start()

    face_extractor_process = Process(target=face_extractor.run, args=(2,))
    face_extractor_process.start()

    embedding_extractor_process = Process(target=embedding_extractor.run, args=(2,))
    embedding_extractor_process.start()
    try:
        while True:
            continue
    except KeyboardInterrupt:
        logger.info(f'Exiting from {__file__}')
        video_loader_process.terminate()
        video_loader_process.join()

        frame_extractor_process.terminate()
        frame_extractor_process.join()

        frame_preprocessor_process.terminate()
        frame_preprocessor_process.join()

        face_extractor_process.terminate()
        face_extractor_process.join()

        embedding_extractor_process.terminate()
        embedding_extractor_process.join()
