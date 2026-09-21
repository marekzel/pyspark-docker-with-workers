FROM apache/spark:3.5.3-python3

USER root

RUN pip install --no-cache-dir pyspark==3.5.7

WORKDIR /opt/project

ENV PYTHONUNBUFFERED=1

CMD ["/opt/spark/bin/spark-submit", "--version"]
