import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))  # /opt/airflow/dags 기준으로 utils 폴더 접근

import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
from airflow.exceptions import AirflowSkipException, AirflowException

def extract_and_upload_to_minio():

    current_date = datetime.now().strftime('%Y-%m-%d')

    base_path = os.path.dirname(os.path.abspath(__file__))
    source_dir = os.path.abspath(os.path.join(base_path, "../../raw-data"))

    bucket_name = 'bronze'
    minio_config = {
        'endpoint_url': 'http://localhost:9000', # Minio address
        'aws_access_key_id': os.getenv("MINIO_ACCESS_KEY"),
        'aws_secret_access_key': os.getenv("MINIO_SECRET_KEY"),
    }

    # check directory's existence
    if not os.path.exists(source_dir):
        raise AirflowException(f"Can't find original directory: {source_dir}")

    # check CSV file's existence
    files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
    if not files:
        raise AirflowSkipException("There isn't valid CSV file.")

    # Create S3 Client
    s3_client = boto3.client('s3', **minio_config)

    # file upload
    for filename in os.listdir(source_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(source_dir, filename)
            folder_name = os.path.splitext(filename)[0]

            try:
                # read initial 200 rows
                df = pd.read_csv(file_path, nrows=200)

                # convert Dataframe to CSV(use memory buffer)
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)

                # set path
                target_key = f"{folder_name}/{current_date}/{filename}"

                # upload to Minio
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=target_key,
                    Body=csv_buffer.getvalue()
                )

                print(f"✅ 전송 완료: {target_key} (데이터 {len(df)}건)")

            except Exception as e:
                print(f"❌ 실패: {filename} | 에러: {e}")

    print(f"\n모든 작업이 완료되었습니다. (기준 날짜: {current_date})")