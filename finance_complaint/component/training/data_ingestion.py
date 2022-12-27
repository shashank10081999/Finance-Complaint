import os
import re
import sys
import time
import uuid
from collections import namedtuple
from typing import List

import json
import pandas as pd
import requests

from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from finance_complaint.entity.config_entity import DataIngestionConfig
from finance_complaint.entity.metadata_entity import DataIngestionMetadata
from finance_complaint.config.spark_manager import spark_session

from datetime import datetime

DownloadUrl = namedtuple("DownloadUrl", ["url", "file_path", "n_retry"])



class DataIngestion():

    def __init__(self , data_ingestion_config: DataIngestionConfig, n_retry: int = 5) -> None:
        """
        data_ingestion_config: Data Ingestion config
        n_retry: Number of retry filed should be tried to download in case of failure encountered
        n_month_interval: n month data will be downloded
        """
        self.data_ingestion_config = data_ingestion_config
        self.failed_urls : List[DownloadUrl] = []
        self.n_retry = n_retry

    def get_required_interval(self):

        start_date = datetime.strptime(self.data_ingestion_config.form_date, "%Y-%m-%d")
        end_date = datetime.strptime(self.data_ingestion_config.to_date, "%Y-%m-%d")

        n_diff_days = (end_date - start_date).days

        freq = None
        if n_diff_days > 365:
            freq = "Y"
        elif n_diff_days > 30:
            freq = "M"
        elif n_diff_days > 7:
            freq = "W"
        
        if freq is None:
            interval = pd.date_range(start=self.data_ingestion_config.from_date,end=self.data_ingestion_config.to_data,
                                periods=2).astype("str").tolist()
        
        else:
            interval = pd.date_range(start=self.data_ingestion_config.form_date,end=self.data_ingestion_config.to_date,
                                freq=freq).astype("str").tolist()

        if self.data_ingestion_config.to_date not in interval:
            interval.append(self.data_ingestion_config.to_date)

        return interval
    

    def download_files(self):

        """
        n_month_interval_url: if not provided then information default value will be set
        =======================================================================================
        returns: List of DownloadUrl = namedtuple("DownloadUrl", ["url", "file_path", "n_retry"])
        """

        try:
            interval = self.get_required_interval()

            for index in range(1,len(interval)):
                from_date , to_date = interval[index-1] , interval[index]

                download_url : str = self.data_ingestion_config.datasource_url
                url : str = download_url.replace("<todate>" , to_date).replace("<fromdate>" , from_date)
                file_name : str = f"{self.data_ingestion_config.file_name}_{from_date}_{to_date}.json"
                file_path : str = os.path.join(self.data_ingestion_config.download_dir,file_name)
                download_url = DownloadUrl(url = url , file_path=file_path , n_retry=self.n_retry)
                self.download_data(download_url = download_url)
        except Exception as e:
            raise e 

    def download_data(self,download_url):
        try:
            download_dir = os.path.dirname(download_url.file_path)
            os.makedirs(download_dir , exist_ok = True)

            print(download_url.url)
            print(download_url.file_path)

            data = requests.get(download_url.url, params={'User-agent': f'your bot {uuid.uuid4()}'})

            try:

                with open(download_url.file_path , "w") as file_obj:
                    finance_complaint_data = list(map(lambda x:x["_source"] , filter(lambda x: "_source" in x.keys() , json.loads(data.content))))

                    json.dump(finance_complaint_data , file_obj)

            except Exception as e:
                
                if os.path.exists(download_url.file_path):
                    os.remove(download_url.file_path)
                self.retry_download_data(data, download_url=download_url)

        except Exception as e:
            raise e


    def retry_download_data(self,data,download_url):
        """
        This function help to avoid failure as it help to download failed file again
        
        data:failed response
        download_url: DownloadUrl
        """
        try:
            if download_url.n_retry == 0:
                self.failed_urls.append(download_url)
                return 
        
            content = data.content.decode("utf-8")
            wait_second = re.findall(r'\d+', content)

            if len(wait_second)>0:
                time.sleep(int(wait_second[0])+2)
            
            failed_file_path = os.path.join(self.data_ingestion_config.failed_dir,os.path.basename(download_url.file_path))

            os.makedirs(self.data_ingestion_config.failed_dir , exist_ok=True)

            with open(failed_file_path, "wb") as failed_file_obj:
                failed_file_obj.write(data.content)
            
            download_url = DownloadUrl(url = download_url.url , file_path= download_url.file_path , n_retry=download_url.n_retry - 1)

            self.download_data(download_url=download_url)

        except Exception as e:
            raise e 

    def convert_files_to_parquet(self) -> str:
        """
        downloaded files will be converted and merged into single parquet file
        json_data_dir: downloaded json file directory
        data_dir: converted and combined file will be generated in data_dir
        output_file_name: output file name 
        =======================================================================================
        returns output_file_path
        """

        try:
            json_data_dir = self.data_ingestion_config.download_dir
            data_dir = self.data_ingestion_config.feature_store_dir
            output_file_name = self.data_ingestion_config.file_name

            os.makedirs(data_dir,exist_ok=True)

            file_path = os.path.join(data_dir, f"{output_file_name}")

            if not os.path.exists(json_data_dir):
                return file_path

            for file_name in os.listdir(json_data_dir):
                json_file_path = os.path.join(json_data_dir,file_name)

                df = spark_session.read.json(json_file_path)
                if df.count() > 0:
                    df.write.mode('append').parquet(file_path)
            return file_path
        except Exception as e:
            raise e
        

    def write_metadata(self, file_path: str) -> None:
        """
        This function help us to update metadata information 
        so that we can avoid redundant download and merging.

        """

        try:

            metadata_info = DataIngestionMetadata(metadata_file_path=self.data_ingestion_config.metadata_file_path)

            metadata_info.write_metadata_info(from_date= self.data_ingestion_config.form_date , to_date=self.data_ingestion_config.to_date,data_file_path=file_path)

        except Exception as e:
            raise e 




    def initiate_data_ingestion(self) -> DataIngestionArtifact:

        try:


            if self.data_ingestion_config.form_date != self.data_ingestion_config.to_date:
                self.download_files()

            if os.path.exists(self.data_ingestion_config.download_dir):
                file_path = self.convert_files_to_parquet()
                self.write_metadata(file_path=file_path)

            feature_store_file_path = os.path.join(self.data_ingestion_config.feature_store_dir,self.data_ingestion_config.file_name)

            data_ingestion_artifact = DataIngestionArtifact(
                    feature_store_file_path=feature_store_file_path,
                    download_dir=self.data_ingestion_config.download_dir,
                    metadata_file_path=self.data_ingestion_config.metadata_file_path)
            return data_ingestion_artifact
        
        except Exception as e:
            raise e 

def main():
    try:
        config = FinanceConfig()
        data_ingestion_config = config.get_data_ingestion_config()
        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config,n_retry=6)
        data_ingestion.initiate_data_ingestion()
    except Exception as e:
        raise e


if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        raise e 

        
        



