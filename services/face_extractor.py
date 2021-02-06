from facenet_pytorch import MTCNN
import cv2
from PIL import Image
import numpy as np
from matplotlib import pyplot as plt
import uuid
import os
import torch


class FaceExtractor:
    def __init__(self, detector_frame_batch_size=30):
        self.detector_frame_batch_size = detector_frame_batch_size
        self.mtcnn = MTCNN(margin=20, keep_all=True, select_largest=False, post_process=False, device='cuda:0')

    @staticmethod
    def _batch(array_, n_chunks):
        """
            Can by used to batch the frames for multiprocessing
        :param array_:
        :param n_chunks:
        :return:
        """
        for i in range(0, len(array_), n_chunks):
            yield array_[i:i + n_chunks]

    def _find_unique(self, faces):
        pass

    def _process_batch_frames(self, frames):
        result = []
        frames = self.mtcnn(frames)
        for frame in frames:
            if isinstance(frame, torch.Tensor):
                for face in frame:
                    result.append(face.permute(1, 2, 0).int().numpy())
        return result

    def process_video(self, path_to_video):
        video = cv2.VideoCapture(path_to_video)
        video_len = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = round(video.get(cv2.CAP_PROP_FPS))
        frames = []
        faces = []  # will be NxMx3xWxL, N-number of frames, M-number of founded faces, W-width of face img L-length
        for i in range(video_len):
            video.grab()
            if i % int(fps) == 0:
                _, frame = video.retrieve()
                # If I have more complicated transform I would rather use multiprocessing for frames list
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                frames.append(Image.fromarray(frame))
                if len(frames) >= self.detector_frame_batch_size:
                    # we don't want to use gpu memory after we detect faces so let's do O(NxM) loop
                    faces.extend(self._process_batch_frames(frames))
                    frames = []
        else:
            if frames:
                faces.extend(self._process_batch_frames(frames))

        for face in faces:
            np.save(f'../faces_numpy/{uuid.uuid4()}', face)


if __name__ == '__main__':
    face_extractor = FaceExtractor(detector_frame_batch_size=5)
    # abofeumbvv.mp4
    for file_path in os.listdir('../videos_origin'):
        print(file_path)
        face_extractor.process_video(path_to_video=f'../videos_origin/{file_path}')
    # face_extractor.process_video('../videos_origin/abofeumbvv.mp4')
