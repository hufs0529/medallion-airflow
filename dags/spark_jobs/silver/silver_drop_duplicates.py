import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))  # /opt/airflow/dags 기준으로 utils 폴더 접근

from utils.spark_runner import SparkJobRunner
from datetime import datetime

files_to_convert = ["olist_customers_dataset", "olist_geolocation_dataset", "olist_order_items_dataset", "olist_order_payments_dataset", "olist_order_reviews_dataset", "olist_orders_dataset", "olist_products_dataset", "olist_sellers_dataset", "product_category_name_translation"]
today_str = datetime.today().strftime("%Y-%m-%d")


def drop_duplicates(spark):

    import logging
    logger = logging.getLogger("drop-duplicates")

    df_customers = spark.read.parquet(f"s3a://silver/olist_customers_dataset/date={today_str}/", header=True, inferSchema=True)
    df_geo = spark.read.parquet(f"s3a://silver/olist_geolocation_dataset/date={today_str}/", header=True, inferSchema=True)
    df_order_items = spark.read.parquet(f"s3a://silver/olist_order_items_dataset/date={today_str}/", header=True, inferSchema=True)
    df_order_payments = spark.read.parquet(f"s3a://silver/olist_order_payments_dataset/date={today_str}/", header=True, inferSchema=True)
    df_order_reviews = spark.read.parquet(f"s3a://silver/olist_order_reviews_dataset/date={today_str}/", header=True, inferSchema=True)
    df_orders = spark.read.parquet(f"s3a://silver/olist_orders_dataset/date={today_str}/", header=True, inferSchema=True)
    df_products = spark.read.parquet(f"s3a://silver/olist_products_dataset/date={today_str}/", header=True, inferSchema=True)
    df_sellers = spark.read.parquet(f"s3a://silver/olist_sellers_dataset/date={today_str}/", header=True, inferSchema=True)
    df_product_category_name = spark.read.parquet(f"s3a://silver/product_category_name_translation/date={today_str}/", header=True, inferSchema=True)

    table_name = "my_catalog.silver.customers_dupl"

    # 1. 중복 제거 로직 (기존 데이터가 있으면 합치고, 없으면 새로 생성)
    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)
        # 기존 데이터 + 신규 데이터 합친 후 중복 제거
        df_result = df_existing.unionByName(df_customers).dropDuplicates(["customer_id", "customer_unique_id"])
        logger.info(f"Table {table_name} existed. Merging data...")
    else:
        df_result = df_customers.dropDuplicates(["customer_id", "customer_unique_id"])
        logger.info(f"Table {table_name} does not exist. Creating new...")

    # 2. Iceberg 테이블에 저장 (writeTo 권장)
    # .save("table_name") (X) -> 변수 table_name 사용 (O)
    df_result.writeTo(table_name).createOrReplace()
    df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/customers_dupl/date={today_str}/")


    # geolocation duplication

    table_name = "my_catalog.silver.geolocation_dupl"

    # 1. 중복 제거 로직 (기존 데이터가 있으면 합치고, 없으면 새로 생성)
    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)
        # 기존 데이터 + 신규 데이터 합친 후 중복 제거
        df_result = df_existing.unionByName(df_geo).dropDuplicates(["geolocation_lat", "geolocation_lng", "geolocation_zip_code_prefix"])
        logger.info(f"Table {table_name} existed. Merging data...")
    else:
        df_result = df_geo.dropDuplicates(["geolocation_lat", "geolocation_lng", "geolocation_zip_code_prefix"])
        logger.info(f"Table {table_name} does not exist. Creating new...")

    # 2. Iceberg 테이블에 저장 (writeTo 권장)
    # .save("table_name") (X) -> 변수 table_name 사용 (O)
    df_result.writeTo(table_name).createOrReplace()
    df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/geolocation_dupl/date={today_str}/")


    # order items duplication
    from pyspark.sql import Window
    import pyspark.sql.functions as F

    window_spec = Window.partitionBy("order_id").orderBy(F.col("order_id").desc())
    df_latest = df_order_items.withColumn("rn", F.row_number().over(window_spec)) \
                .filter("rn = 1") \
                .drop("rn")

    table_name = "my_catalog.silver.order_items_dupl"

    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)
        df_combined = df_existing.unionByName(df_latest)

        df_combined.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        df_combined.write.mode("overwrite").parquet(f"s3a://silver/dupl/order_item_dupl/date={today_str}/")

        logger.info(f"Table {table_name} existed. Data merged and overwritten.")
    else:
        df_latest.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        df_latest.write.mode("overwrite").parquet(f"s3a://silver/dupl/order_item_dupl/date={today_str}/")

        logger.info(f"Table {table_name} did not exist. Table created and data inserted.")

    # order payments duplication

    df_order_payments.createOrReplaceTempView("new_batch")

    table_name = "my_catalog.silver.order_payments_dupl"

    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)

        spark.sql("""
            MERGE INTO my_catalog.silver.order_payments_dupl AS target
            USING new_batch AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        df_result = spark.table(table_name)
        df_result.write.mode("overwrite").parquet(
        "s3a://silver/dupl/order_payment_dupl/"
        )
        logger.info(f"Table {table_name} existed. Data merged and overwritten.")
    else:
        df_order_payments.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        df_order_payments.write.mode("overwrite").parquet(
        "s3a://silver/dupl/order_payment_dupl/"
        )


    # order reviews

    table_name = "my_catalog.silver.order_reviews_dupl"

    # 1. 중복 제거 로직 (기존 데이터가 있으면 합치고, 없으면 새로 생성)
    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)
        # 기존 데이터 + 신규 데이터 합친 후 중복 제거
        df_result = df_existing.unionByName(df_order_reviews).dropDuplicates(["order_id", "review_id"])
        logger.info(f"Table {table_name} existed. Merging data...")
    else:
        df_result = df_order_reviews.dropDuplicates(["order_id", "review_id"])
        logger.info(f"Table {table_name} does not exist. Creating new...")

    df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/order_review_dupl/date={today_str}/")
    # 2. Iceberg 테이블에 저장 (writeTo 권장)
    # .save("table_name") (X) -> 변수 table_name 사용 (O)
    df_result.writeTo(table_name).createOrReplace()


    df_orders.createOrReplaceTempView("batch")

    table_name = "my_catalog.silver.orders_dupl"

    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)

        spark.sql("""
            MERGE INTO my_catalog.silver.orders_dupl AS target
            USING batch AS source
            ON target.order_id = source.order_id AND target.customer_id = source.customer_id
            WHEN MATCHED THEN 
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        df_result = spark.table(table_name)
        df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/order_dupl/date={today_str}/")
        
        logger.info(f"Table {table_name} existed. Data merged and overwritten.")
    else:
        df_orders.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/order_dupl/date={today_str}/")
        
        logger.info(f"Table {table_name} did not exist. Table created and data inserted.")


    df_products.createOrReplaceTempView("batch")

    table_name = "my_catalog.silver.products_dupl"

    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)

        spark.sql("""
            MERGE INTO my_catalog.silver.products_dupl AS target
            USING batch AS source
            ON target.product_id = source.product_id
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
            """)
        df_result = spark.table(table_name)
        df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/product_dupl/date={today_str}/")

        logger.info(f"Table {table_name} existed. Data merged and overwritten.")
    else:
        df_products.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        df_products.write.mode("overwrite").parquet(f"s3a://silver/dupl/product_dupl/date={today_str}/")

        logger.info(f"Table {table_name} did not exist. Table created and data inserted.")


    # sellers

    table_name = "my_catalog.silver.sellers_dupl"

    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)
        df_result = df_existing.unionByName(df_sellers).dropDuplicates(["seller_id"])
        logger.info(f"Table {table_name} existed. Merging data...")
    else:
        df_result = df_sellers.dropDuplicates(["seller_id"])
        logger.info(f"Table {table_name} does not exist. Creating new...")

    df_result.writeTo(table_name).createOrReplace()
    df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/seller_dupl/date={today_str}/")
    logger.info(f"Successfully saved to {table_name}")


    # product_category

    table_name = "my_catalog.silver.product_category_dupl"

    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)
        df_result = df_existing.unionByName(df_product_category_name).dropDuplicates(["product_category_name"])
        logger.info(f"Table {table_name} existed. Merging data...")
    else:
        df_result = df_product_category_name.dropDuplicates(["product_category_name"])
        logger.info(f"Table {table_name} does not exist. Creating new...")

    df_result.writeTo(table_name).createOrReplace()
    df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/product_category_dupl/date={today_str}/")
    logger.info(f"Successfully saved to {table_name}")


    # order payments duplication

    df_order_payments.createOrReplaceTempView("new_batch")

    table_name = "my_catalog.silver.order_payments_dupl"

    if spark.catalog.tableExists(table_name):
        df_existing = spark.table(table_name)

        spark.sql("""
            MERGE INTO my_catalog.silver.order_payments_dupl AS target
            USING new_batch AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        df_result = spark.table(table_name)
        df_result.write.mode("overwrite").parquet(f"s3a://silver/dupl/order_payment_dupl/date={today_str}/")
        logger.info(f"Table {table_name} existed. Data merged and overwritten.")
    else:
        df_order_payments.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        df_order_payments.write.mode("overwrite").parquet(f"s3a://silver/dupl/order_paymentdupl/date={today_str}/")

        logger.info(f"Table {table_name} did not exist. Table created and data inserted.")

if __name__ == "__main__":

    runner = SparkJobRunner("drop_duplicates")

    runner.run(drop_duplicates)