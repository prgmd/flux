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

# 호스트 테스트용. Spark 잡에는 필요 없지만, 번역 계층 테스트(-m spark)를
# 이 이미지 안에서 돌린다. 호스트에는 JDK 17+가 없어 Spark가 뜨지 않는다.
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

# 스크립트가 전부 `from flux... import`로 시작한다. 이것이 없으면
# 어떤 잡도 ModuleNotFoundError로 죽는다.
#
# COPY와 PYTHONPATH를 둘 다 둔다. COPY는 이미지 단독으로 돌게 하고,
# PYTHONPATH는 개발 중 저장소를 /opt/flux에 마운트해 덮어썼을 때도
# 같은 자리를 가리키게 한다. 둘 중 하나만 두면 한쪽 사용법이 깨진다.
COPY pyproject.toml pytest.ini ./
COPY src ./src
COPY scripts ./scripts
COPY tests ./tests

ENV PYTHONPATH=/opt/flux/src

CMD ["python", "-c", "import flux, pyspark; print(pyspark.__version__)"]
