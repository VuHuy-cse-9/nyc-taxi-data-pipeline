FROM apache/spark:4.0.2-java21

# Copy startup script
COPY start-spark.sh /opt/start-spark.sh

COPY jars/* /opt/spark/jars

COPY test.py /opt/spark/work-dir

# Default command to start Spark master + worker
CMD ["/opt/start-spark.sh"]
