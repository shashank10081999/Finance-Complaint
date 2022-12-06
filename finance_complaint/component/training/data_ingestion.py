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
        self.failed_urls : list(DownloadUrl) = []
        self.n_retry = n_retry

    def get_required_interval(self):

        start_date = datetime.strptime(self.data_ingestion_config.from_date, "%Y-%m-%d")
        end_date = datetime.strptime(self.data_ingestion_config.to_date, "%Y-%m-%d")

        n_diff_days = (end_date - start_date).days

        freq = None
        if n_diff_days > 365:
            freq = "Y"
        if n_diff_days > 30:
            freq = "M"
        if n_diff_days > 7:
            freq = "W"
        
        if freq is None:
            interval = pd.date_range(start=self.data_ingestion_config.from_date,end=self.data_ingestion_config.to_data,
                                periods=2).astype("str").tolist()
        
        else:
            interval = pd.date_range(start=self.data_ingestion_config.from_date,end=self.data_ingestion_config.to_data,
                                freq=freq).astype("str").tolist()

        if self.data_ingestion_config.to_data not in interval:
            interval.append(self.data_ingestion_config.to_data)

        return interval
    

    def download_files():
        pass


