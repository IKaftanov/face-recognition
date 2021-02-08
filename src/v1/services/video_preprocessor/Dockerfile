FROM python:3

WORKDIR /app

ADD v1/services/video_preprocessor/requirements.txt .

RUN apt-get update ##[edited]
RUN apt-get install ffmpeg libsm6 libxext6  -y
RUN pip install torch torchvision
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "v1.services.video_preprocessor.video_preprocessor"]
