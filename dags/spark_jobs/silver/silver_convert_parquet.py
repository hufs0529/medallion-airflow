import os
import sys
from datetime import datetime

# 루트 경로 추가 (Airflow 컨테이너 기준)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))  # /opt/airflow/dags 기준으로 utils 폴더 접근

from utils.spark_runner import SparkJobRunner


def convert_parquet(spark):
    """
    CSV → Iceberg silver table 변환
    spark : SparkSession 인스턴스
    """
    today = datetime.today().strftime("%Y-%m-%d")

    files = [
        "olist_customers_dataset",
        "olist_geolocation_dataset",
        "olist_order_items_dataset",
        "olist_order_payments_dataset",
        "olist_order_reviews_dataset",
        "olist_orders_dataset",
        "olist_products_dataset",
        "olist_sellers_dataset",
        "product_category_name_translation"
    ]

    import logging
    logger = logging.getLogger("covert-parquet")

    for file in files:
        csv_path = f"s3a://bronze/{file}/{today}/{file}.csv"

        try:
            df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
            spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.silver")
            # Iceberg silver 테이블 생성 (overwrite)
            df.writeTo(f"my_catalog.silver.{file}").using("iceberg").createOrReplace()
            #df.write.mode("overwrite").parquet(f"s3a://silver/{file}/{today}")
# "s3a://silver/olist_customers_dataset/date={today_str}/"
# df_combined.write.mode("overwrite").parquet(f"s3a://silver/dupl/order_item_dupl/date={today_str}/")
            logger.info(f"[SUCCESS] {file} → Iceberg silver 저장 완료")

        except Exception as e:
            logger.error(f"[ERROR] {file} → {e}")


if __name__ == "__main__":

    runner = SparkJobRunner("convert_parquet")

    runner.run(convert_parquet)