# docker-compose build collector_v2_3
# docker-compose run --rm collector_v2_3 bash

# docker tag icandid_collector_v2_3 registry.docker.libis.be/libis/icandid_collector:v2.3
# docker push registry.docker.libis.be/libis/icandid_collector:v2.3


docker-compose run --rm collector_v2_3 ruby /app/src/zweedsparlement_download.rb
docker-compose run --rm collector_v2_3 ruby /app/src/zweedsparlement_parser.rb
