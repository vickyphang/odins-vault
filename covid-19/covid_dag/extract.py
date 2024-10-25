import pandas as pd
import datetime as dt
import logging
import boto3
from io import StringIO

class Extract:
    COL_LIST = ['date','location_iso_code','location','new_cases','new_deaths','new_recovered','new_active_cases','total_cases','total_deaths','total_recovered','total_active_cases','location_level','city_or_regency','province','country','continent','island','time_zone','special_status','total_regencies','total_cities','total_districts','total_urban_villages','total_rural_villages','area','population','population_density','longitude','latitude','new_cases_per_million','total_cases_per_million','new_deaths_per_million','total_deaths_per_million','case_fatality_rate','case_recovered_rate','growth_factor_of_new_cases','growth_factor_of_new_deaths']

    def __init__(self, date : dt.datetime) -> None:
        self.date = self.process_time(date)
        self.bucket_name = 'coviddata'
        self.object_key = 'covid_19_indonesia_time_series_all.csv'
        logging.basicConfig(level=logging.INFO)
        self.s3_client = boto3.client('s3', endpoint_url='https-url')  # Initialize S3 client

    def process_time(self,time: dt.datetime):
        """Round the supplied time to date"""
        return time.replace(second=0, microsecond=0, minute=0, hour=0)

    def download_csv_from_s3(self) -> str:
        """Download the CSV file from the S3 bucket"""
        try:
            logging.info(f"Downloading file from S3: bucket={self.bucket_name}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.object_key)
            csv_content = response['Body'].read().decode('utf-8')
            logging.info(f"File downloaded successfully from S3.")
            return csv_content
        except Exception as e:
            logging.error(f"Error downloading file from S3: {e}")
            raise

    def execute_extraction(self) -> pd.DataFrame:
        try:
            # Download CSV content from S3
            csv_content = self.download_csv_from_s3()

            # Load the CSV content into a pandas DataFrame
            df = pd.read_csv(StringIO(csv_content), usecols=self.COL_LIST)
            #df = df[df.date == self.date]
            logging.info(f"Extraction completed, {len(df)} records found.")
            return df
        except Exception as e:
            logging.error(f"Error occurred during extraction: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on failure