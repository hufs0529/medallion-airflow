import sys
import os

# utils import 경로
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from utils.spark_runner import SparkJobRunner

# silver job import
from silver_convert_parquet import convert_parquet
from silver_drop_duplicates import drop_duplicates
from silver_join import table_merge


def run_all_jobs(spark):
    """
    하나의 SparkSession에서 silver 단계 전체 실행
    """
    convert_parquet(spark)
    drop_duplicates(spark)
    table_merge(spark)


if __name__ == "__main__":
    runner = SparkJobRunner("silver-all-jobs")
    runner.run(run_all_jobs)