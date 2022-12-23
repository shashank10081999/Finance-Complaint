import sys

from finance_complaint.exception import FinanceException
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame

import shutil
import os
from finance_complaint.constant.model import MODEL_SAVED_DIR
import time
from typing import List, Optional
import re
from abc import abstractmethod, ABC
from finance_complaint.config.aws_connection_config import AWSConnectionConfig



class CloudEstimator(ABC):

    key = "model-registry"
    model_dir = "saved_model"
    compressed_file_format = "zip"
    model_file_name = "model.zip"

    @abstractmethod
    def get_all_model_path(self, key) -> List[str]:

        """
        Args:
            key: cloud storage key where model is location
            Returns: return List of all model cloud storage key path
        """
        pass
    
    @abstractmethod
    def get_latest_model_path(self,key) -> str:

        """
        This function will return the latest model path from the cloud storage

        Args:
            key: Location in the cloud where all the model are storge 
        """

        pass 
    
    @abstractmethod
    def decompress_model(self, zip_model_file_path, extract_dir) -> str:

        """
        This function extract downloaded zip model from cloud storage

        Args:
            Zip_model_file_path: Path of zip file 
            extract_dir: Path where the extract 


        """
        pass
    
    @abstractmethod
    def compress_model_dir(self,model_dir):
        """
        This will compress the models folder , so we can send it to any cloud platform AWS , AZURE 
        Args:
            model_dir : folder to compress 
        """
        pass
    
    @abstractmethod
    def is_model_available(self,key):
        """
        This function will return if the model exist or not 
        Args :
            key - cloud storage key is where model location 
        """
        pass
    
    @abstractmethod
    def save(self,model_dir,key):
        """
        This function will compress the folder 1st and the upload the folder in the cloud
        Args:
            model_dir : model folder to compress 
            key : location in the could to upload 
        """
        pass 
    
    @abstractmethod
    def load(self,key,extract_dir):
        """
        This function download the latest  model if complete cloud storage key for model not provided else
        model will be downloaded using provided model key path in extract dir
        Args:
            key - loaction in cloud stoage 
            extract_dir - dir to download in the local system 
        """
        pass
    
    @abstractmethod
    def transform(self):
        """
        This function is designed to be overridden by child class
        Args:
            df: Input dataframe

        Returns:

        """
        pass
    

class S3Estimator(CloudEstimator):

    def __init__(self, bucket_name, region_name="ap-south-1", **kwargs):
        """

        Args:
            bucket_name: s3 bucket name
            region_name: s3 bucket region
            **kwargs: other keyword argument
        """

        if len(kwargs)>0:
            super().__init__(kwargs)
        
        aws_connection_config = AWSConnectionConfig(region_name = region_name)

        self.s3_client = aws_connect_config.s3_client
        self.resource =aws_connect_config.s3_resource

        response  = self.s3_client.list_buckets()

        available_buckets = [bucket["Name"] for bucket in response["Buckets"]]

        if bucket_name not in available_buckets:
            location = {'LocationConstraint': region_name}
            self.s3_client.create_bucket(Bucket=bucket_name,
                                         CreateBucketConfiguration=location)
        
        self.bucket = self.resource.Bucket(bucket_name)
        self.bucket_name = bucket_name

        self.__model_dir = self.model_dir
        self.__timestamp = str(time.time())[:10]
        self.__model_file_name = self.model_file_name
        self.__compression_format = self.compression_format
    
    def __get_save_model_path(selfkey: str = None) -> str:
        pass

        
