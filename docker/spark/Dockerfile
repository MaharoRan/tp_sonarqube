FROM apache/spark:3.5.7

USER root

RUN apt-get update \
	&& apt-get install -y python3-venv \
	&& rm -rf /var/lib/apt/lists/* \
	&& python3 -m venv /opt/venv \
	&& /opt/venv/bin/python -m pip install --no-cache-dir psycopg2-binary==2.9.9

ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

COPY spark/sales_stream.py /opt/spark-app/sales_stream.py

WORKDIR /opt/spark-app
