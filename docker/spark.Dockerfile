FROM python:3.14-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/flux

COPY requirements-spark.txt .
RUN pip install --no-cache-dir -r requirements-spark.txt

RUN ln -s "$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

CMD ["python", "-c", "import pyspark; print(pyspark.__version__)"]
