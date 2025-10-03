# Author: Tom Vanmechelen @ KU Leuven (2025). Apache 2.0 License
FROM python:3.13.5-bookworm

RUN addgroup app --gid 10000 && adduser app --home /app --uid 10000 --ingroup app
USER app:app
RUN pip install pyreadstat
RUN pip install pandas


WORKDIR /app

ADD ./src/ ./src

WORKDIR /app/src
