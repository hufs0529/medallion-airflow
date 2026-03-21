from pyspark.sql import SparkSession
import os
import logging
import time

class SparkJobRunner:

    def __init__(self, app_name):
        self.app_name = app_name
        self.spark = None
        self.logger = self._create_logger()

    def _create_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        return logging.getLogger(self.app_name)
    
    def _create_spark(self):
        minio_access = os.getenv("MINIO_ACCESS_KEY")
        minio_secret = os.getenv("MINIO_SECRET_KEY")
        self.logger.info("Creating SparkSession")

        self.spark = (
            SparkSession.builder
            .appName(self.app_name)
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4"
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            .config("spark.sql.catalog.my_catalog",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.my_catalog.type", "hadoop")
            .config("spark.sql.catalog.my_catalog.warehouse",
                    "s3a://iceberg-warehouse")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", minio_access)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.timeout", 600000)  # ms 단위
            .config("spark.hadoop.fs.s3a.socket.timeout", 600000)
            .getOrCreate()
        )

    def run(self, job_func):

        start = time.time()

        try:
            self._create_spark()

            self.logger.info("Job started")

            job_func(self.spark)

            self.logger.info("Job success")

        except Exception as e:
            self.logger.error(f"Job failed: {e}")
            raise

        finally:
            if self.spark:
                self.spark.stop()

            self.logger.info(f"Elapsed: {time.time() - start:.2f}s")