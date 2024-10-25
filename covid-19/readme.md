# ETL for COVID-19 Data with Airflow

References: https://medium.com/@shakedm795/learn-airflow-by-making-an-etl-for-covid-19-data-with-bigquery-c9aeb67bbab6

Step:
- upload covid data to S3
- add ~/.aws/credentials to airflow VM
- install airflow[postgres]
- run: pip3 install pandas datetime boto3 sqlalchemy numpy (future:: make requirments.txt)
- add new airflow connection: citus
- upload dag
- trigger dag