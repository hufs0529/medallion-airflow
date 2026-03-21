import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))  # /opt/airflow/dags 기준으로 utils 폴더 접근

from utils.spark_runner import SparkJobRunner
from datetime import datetime

today_str = datetime.today().strftime("%Y-%m-%d")

def table_merge(spark):

    import logging
    logger = logging.getLogger("table-merge")
    
    df_customer = spark.read.parquet(f"s3a://silver/dupl/customers_dupl/date={today_str}/", header=True, inferSchema=True)
    df_geolocation = spark.read.parquet(f"s3a://silver/dupl/geolocation_dupl/date={today_str}/", header=True, inferSchema=True)
    df_order = spark.read.parquet(f"s3a://silver/dupl/order_dupl/date={today_str}/", header=True, inferSchema=True)
    df_order_item = spark.read.parquet(f"s3a://silver/dupl/order_item_dupl/date={today_str}/", header=True, inferSchema=True)
    df_order_payment = spark.read.parquet(f"s3a://silver/dupl/order_payment_dupl/date={today_str}/", header=True, inferSchema=True)
    df_order_review = spark.read.parquet(f"s3a://silver/dupl/order_review_dupl/date={today_str}/", header=True, inferSchema=True)
    df_product_category = spark.read.parquet(f"s3a://silver/dupl/product_category_dupl/date={today_str}/", header=True, inferSchema=True)
    df_product = spark.read.parquet(f"s3a://silver/dupl/product_dupl/date={today_str}/", header=True, inferSchema=True)
    df_seller = spark.read.parquet(f"s3a://silver/dupl/seller_dupl/date={today_str}/", header=True, inferSchema=True)

    # orderItem + product
    # product + proudct_category
    from pyspark.sql.functions import broadcast

    table_name = "my_catalog.silver.orderitem_product_merge"

    df_orderItem_product = df_order_item.join(
        df_product,
        "product_id", 
        "inner"
    )
    df_orderItem_product.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    df_orderItem_product.write.mode("overwrite").parquet(f"s3a://silver/join/orderitem_product/date={today_str}/")

    table_name = "my_catalog.silver.prodct_category_merge"

    df_product_productCategory = df_product.join(broadcast(df_product_category), "product_category_name")
    df_product_productCategory.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    df_product_productCategory.write.mode("overwrite").parquet(f"s3a://silver/join/prodct_category_merge/date={today_str}/")

    spark.sql("select * from my_catalog.silver.prodct_category_merge limit 5").show(5)


    # order + customer
    # order + orderPayment
    # order + orderReview

    from pyspark.sql import functions as F

    # 1. order_payments aggregation
    df_payment_agg = df_order_payment.groupBy("order_id").agg(
        F.sum("payment_value").alias("total_payment"),
        F.count("*").alias("payment_count")
    )

    # 2. order_reviews aggregation
    df_review_agg = df_order_review.groupBy("order_id").agg(
        F.count("*").alias("review_count"),
        F.avg("review_score").alias("avg_review_score")
    )

    df_order_customer = df_order.join(
        df_customer,
        "customer_id",
        "inner")

    table_name = "my_catalog.silver.order_customer_merge"

    df_order_customer.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    df_order_customer.write.mode("overwrite").parquet(f"s3a://silver/join/order_customer_merge/date={today_str}/")


    df_orderPayment = df_order.join(
        df_payment_agg,
        "order_id",
        "inner")

    table_name = "my_catalog.silver.order_payment_merge"

    df_orderPayment.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    df_orderPayment.write.mode("overwrite").parquet(f"s3a://silver/join/order_payment_merge/date={today_str}/")

    df_orderReview = df_order.join(
        df_review_agg,
        "order_id",
        "inner")

    table_name = "my_catalog.silver.order_review_merge"

    df_orderReview.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    df_orderReview.write.mode("overwrite").parquet(f"s3a://silver/join/order_review_merge/date={today_str}/")



    # order + orderItem
    # orderItem + seller
    # customer + geolocation
    from pyspark.sql.functions import broadcast

    df_order.createOrReplaceTempView("order")
    df_order_item.createOrReplaceTempView("order_item")
    df_customer.createOrReplaceTempView("customer")
    df_geolocation.createOrReplaceTempView("geolocation")

    df_orderItem = spark.sql("""
                            select *
                            from order o
                            join order_item i
                            on o.order_id == i.order_id
                            """)

    df_ordeItem_seller = df_order_item.join(broadcast(df_seller), "seller_id")

    table_name = "my_catalog.silver.order_item_merge"

    df_product_productCategory.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    df_product_productCategory.write.mode("overwrite").parquet(f"s3a://silver/join/order_item_merge/date={today_str}/")

    df_customer_geo = spark.sql("""
                                select *
                                from customer c
                                join geolocation g
                                on c.customer_zip_code_prefix == g.geolocation_zip_code_prefix
                                """)
    table_name = "my_catalog.silver.customer_geo_merge"

    df_customer_geo.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    df_customer_geo.write.mode("overwrite").parquet(f"s3a://silver/join/customer_geo_merge/date={today_str}/")


if __name__ == "__main__":

    runner = SparkJobRunner("table_merge")

    runner.run(table_merge)