FROM apache/airflow:3.1.8

# 환경변수
ENV ICEBERG_VERSION=1.4.2
ENV AWS_SDK_VERSION=1.12.262

USER root
# 1. Java 설치 (SparkSubmitOperator 실행 필수)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# 2. pip 업그레이드 및 Provider 패키지 설치
# RUN pip install --no-cache-dir --upgrade pip
# RUN pip install --no-cache-dir \
RUN pip install \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-cncf-kubernetes" \
    "boto3" \
    "pandas"

# 3. Hadoop AWS, Iceberg jar 설치 (SparkSubmit에서 사용)
# SparkSubmitOperator 실행 시 --packages 로도 가능하지만, 컨테이너에 미리 설치하면 편리
RUN mkdir -p /opt/airflow/jars && \
    cd /opt/airflow/jars && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.3_2.12-${ICEBERG_VERSION}.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# 4. SparkSubmitOperator에서 사용할 수 있도록 환경변수에 추가
# ENV SPARK_CLASSPATH=/opt/airflow/jars/*
ENV SPARK_DIST_CLASSPATH="/opt/airflow/jars/*"