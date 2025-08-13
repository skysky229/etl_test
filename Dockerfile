FROM docker.io/library/spark:3.5.0

COPY spark_jobs/jobs/ /opt/spark-apps/

USER root

RUN chown -R spark:spark /opt/spark-apps && chmod 755 -R /opt/spark-apps