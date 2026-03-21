import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))  # /opt/airflow/dags 기준으로 utils 폴더 접근

from utils.spark_runner import SparkJobRunner
from datetime import datetime

today_str = datetime.today().strftime("%Y-%m-%d")

def stats_gold(spark):
    df_orderitem_product = spark.table("my_catalog.silver.orderitem_product_merge")
    df_product_category = spark.table("my_catalog.silver.prodct_category_merge")
    df_order_customer = spark.table("my_catalog.silver.order_customer_merge")
    df_order_payment = spark.table("my_catalog.silver.order_payment_merge")
    df_order_review = spark.table("my_catalog.silver.order_review_merge")
    df_customer_geo = spark.table("my_catalog.silver.customer_geo_merge")

    # Gold: fact_sales

    # using 
    # orderitem_product_merge
    # order_payment_merge
    # order_review_merge
    # order_customer_merge
    # prodct_category_merge

    from pyspark.sql import functions as F

    df_fact_sales = (
        df_orderitem_product
        .drop("product_category_name")
        .join(df_product_category.select("product_id","product_category_name"), "product_id", "left")
        .join(df_order_payment.select("order_id","total_payment","payment_count"), "order_id", "left")
        .join(df_order_review.select("order_id","avg_review_score","review_count"), "order_id", "left")
        .join(df_order_customer.select("order_id","customer_id","order_purchase_timestamp"), "order_id", "left")
    )

    table_name = "my_catalog.gold.fact_sales"

    df_fact_sales.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(table_name)

    df_fact_sales.write.mode("overwrite") \
        .parquet(f"s3a://gold/fact_sales/date={today_str}/")

    # Gold: dim_product

    # using
    # prodct_category_merge

    df_dim_product = (
        df_product_category
        .select(
            "product_id",
            "product_category_name",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        )
        .dropDuplicates()
    )

    table_name = "my_catalog.gold.dim_product"

    df_dim_product.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(table_name)


    # Gold: dim_customer

    # using 
    # customer_geo_merge

    df_dim_customer = (
        df_customer_geo
        .select(
            "customer_id",
            "customer_city",
            "customer_state",
            "geolocation_lat",
            "geolocation_lng"
        )
        .dropDuplicates()
    )

    table_name = "my_catalog.gold.dim_customer"

    df_dim_customer.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(table_name)



    # Gold: product_sales

    # using
    # fact_sales

    df_product_sales = (
        df_fact_sales
        .groupBy("product_id","product_category_name")
        .agg(
            F.sum("price").alias("total_sales"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("avg_review_score").alias("avg_rating")
        )
    )

    table_name = "my_catalog.gold.product_sales"

    df_product_sales.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(table_name)


    # Gold: daily_sales

    # using
    # fact_sales
    df_daily_sales = (
        df_fact_sales
        .groupBy(F.to_date("order_purchase_timestamp").alias("date"))
        .agg(
            F.sum("price").alias("daily_sales"),
            F.countDistinct("order_id").alias("orders"),
            F.avg("avg_review_score").alias("avg_rating")
        )
    )

    table_name = "my_catalog.gold.daily_sales"

    df_daily_sales.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(table_name)

if __name__ == "__main__":

    runner = SparkJobRunner("stats_gold")

    runner.run(stats_gold)

# Bronze
#  └ raw csv

# Silver
#  ├ orderitem_product_merge
#  ├ prodct_category_merge
#  ├ order_customer_merge
#  ├ order_payment_merge
#  ├ order_review_merge
#  └ customer_geo_merge

# Gold
#  ├ fact_sales
#  │   ↑
#  │   ├ orderitem_product_merge
#  │   ├ prodct_category_merge
#  │   ├ order_payment_merge
#  │   ├ order_review_merge
#  │   └ order_customer_merge
#  │
#  ├ dim_product
#  │   └ prodct_category_merge
#  │
#  ├ dim_customer
#  │   └ customer_geo_merge
#  │
#  ├ product_sales
#  │   └ fact_sales
#  │
#  └ daily_sales
#      └ fact_sales


