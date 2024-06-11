ARG REPO_LOCATION=registry.docker.libis.be/
ARG BASE_VERSION=v2.3
FROM ${REPO_LOCATION}libis/icandid_collector:${BASE_VERSION}

WORKDIR $APP_HOME
COPY src ./src
RUN ls -l /app/src/

