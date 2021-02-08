
from `src` folder do \
`docker build -t bulk-processor -f v0/Dockerfile .` \
then do \
`docker run -dit -P --name bulk-processor --network="host" --mount type=bind,source=C:/videos,target=/app/v0/videos bulk-processor`