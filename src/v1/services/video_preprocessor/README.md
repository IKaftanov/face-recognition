In `src` folder do \
`docker build -t video-preprocessor-service -f v1/services/video_preprocessor/Dockerfile .` \
then \
`docker run -dit --name video-preprocessor-service video-preprocessor-service`