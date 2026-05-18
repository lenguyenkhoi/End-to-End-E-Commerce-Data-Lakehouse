from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Cấu hình mặc định
default_args = {
    'owner': 'khoi_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Định nghĩa DAG
with DAG(
    'ecommerce_medallion_batch',
    default_args=default_args,
    description='Luồng đồng bộ bảng DIM định kỳ mỗi ngày',
    schedule_interval='*/5 * * * *', # Chạy 5 phút
    catchup=False,
    tags=['spark', 'bigquery', 'medallion'],
) as dag:

    # Task 1: In ra thông báo bắt đầu
    start_job = BashOperator(
        task_id='start_job',
        bash_command='echo "🚀 Bắt đầu chu kỳ ETL Batch cho các bảng Dimension..."'
    )

    # Task 2: Gọi Spark Submit để chạy file spark_batch.py
    # Lưu ý: Lệnh này chạy bên trong container Airflow Scheduler
    run_spark_batch = BashOperator(
        task_id='run_spark_batch_etl',
        # bash_command="""
        # spark-submit \
        # --master local[*] \
        # --packages com.google.cloud.spark:spark-3.5-bigquery:0.36.1,org.postgresql:postgresql:42.7.2 \
        # /app/3_spark_processing/job/spark_batch.py
        # """
        bash_command='python /app/3_spark_processing/job/spark_batch.py'
    )

    # Task 3: Thông báo hoàn tất
    end_job = BashOperator(
        task_id='end_job',
        bash_command='echo "✅ Toàn bộ bảng DIM đã được cập nhật trên BigQuery!"'
    )

    # Thứ tự thực hiện
    start_job >> run_spark_batch >> end_job