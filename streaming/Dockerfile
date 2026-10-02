FROM apache/spark:3.5.3-python3

USER root

COPY spark_job.py /opt/spark/work-dir/spark_job.py

WORKDIR /opt/spark/work-dir

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", "spark_job.py"]