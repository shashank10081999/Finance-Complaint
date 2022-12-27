import sys
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
    
    def __get_save_model_path(self , key: str = None) -> str:
        """
        This function prepare new cloud storage key to save the zipped mode
        Args:
            key:

        Returns: prepared complete cloud storage key to load model
        """

        if key is None :
            key = self.key
        if not key.endwith("/"):
            key = f"{key}/"
        
        return f"{key}{self.__model_dir}/{self.__timestamp}/{self.__model_file_name}"
        

    def get_all_model_path(self , key: str) -> List[str]:
        """
        This function return list of all available model key
        Args:
        Returns: return List of all model cloud storage key path

        """
        if key is None :
            key = self.key

        if not key.endwith("/"):
            key = f"{key}/"
        
        key = f"{key}{self.__model_dir}/"

        paths =  [obj.key for obj in self.bucket.objects.filter(Prefix = key)]

        return paths

    def get_latest_model_path (self,key : str) -> str:

        if key is None:
            key = self.key 
        
        if not key.endwith("/"):
            key = f"{key}/"
        
        key = f"{key}/{self.__model_dir}"

        timestamps = []

        for key_summary in self.bucket.objects.filter(Prefix = key):
            temp_key = key_summary.key
            timestamp = re.findall("/d" , temp_key)

            timestamps.extend(int(timestamp))
        
        if len(timestamps) == 0:
            return None 

        model_path =  f"{key}/{max(timestamps)}/{self.__model_file_name}"

        return model_path
    
    def decompress_model(self,zip_model_file_path, extract_dir) -> None:
        os.makedir(extract_dir , exist_ok=True)
        shutil.unpack_archive(filename=zip_model_file_path,
                              extract_dir=extract_dir,
                              format=self.__compression_format)
    
    def compress_model_dir(self,model_dir : str):

        if not  os.path.exists(model_dir):
            raise "The given Folder does exists , Please check"
        
        # preparing temp model zip file path
        temp_model_file_path = os.ptha.join(os.getcwd() , f"temp_{self.__timestamp}" ,self.__model_file_name.replace(f".{self.__compression_format}", ""))

        # remove tmp model zip file path is already present
        if os.path.exists(tmp_model_file_name):
            os.remove(tmp_model_file_name)

        # creating zip file of model dir at tmp model zip file path
        shutil.make_archive(base_name=tmp_model_file_name,
                            format=self.__compression_format,
                            root_dir=model_dir
                            )
        tmp_model_file_name = f"{tmp_model_file_name}.{self.__compression_format}"

        return tmp_model_file_name
    
    def save(self, model_dir , key) -> None:
        """

        This function save provide model dir to cloud
        It internally compress the model dir then upload it to cloud
        Args:
            model_dir: Model dir to compress
            key: cloud storage key where model will be saved

        Returns:


        """

        if not os.path.exists(model_dir):
            raise "The model dir path doesnot exits , Please check the folder name"
        
        model_zip_file_path = self.compress_model_dir(model_dir = model_dir)
        save_model_path = self.__get_save_model_path(key=key)
        self.s3_client.upload_file(model_zip_file_path, self.bucket_name, save_model_path)
        shutil.rmtree(os.path.dirname(model_zip_file_path))
    
    def is_model_available(self,key) -> bool:
        """

        Args:
            key: Cloud storage to key to check if model available or not
        Returns:

        """
        return bool(len(self.get_all_model_path(key)))
    
    def load(self, key , extract_dir) -> str:

        """
        This function download the latest  model if complete cloud storage key for model not provided else
        model will be downloaded using provided model key path in extract dir
        Args:
            key:
            extract_dir:

        Returns: Model Directory
        """
        if not key.endwith(self.__model_file_name):
            model_path = self.get_latest_model_path(key = key)
            if model_path is None:
                raise "Model does not exists in the cloud , Please check"
        else:
            model_path = key
        
        timestamp = re.findall("/d", model_path)[0]
        extract_dir = os.path.join(extract_dir,timestamp)
        os.makedirs(extract_dir , exist_ok=True)

        download_file_path = os.path.join(extract_dir ,self.__model_file_name)

        self.s3_client.download_file(elf.bucket_name, model_path, download_file_path)

        self.decompress_model(zip_model_file_path = download_file_path, extract_dir =extract_dir)

        os.remove(download_file_path)

        model_dir = os.path.join(extract_dir, os.listdir(extract_dir)[0])
        return model_dir

    @abstractmethod
    def transform(self, df) -> DataFrame:
        pass


class FinanceComplaintEstimator():
    def __init__(self,**kwargs):
        self.model_dir = MODEL_SAVED_DIR
        self.loaded_model_path = None
        self.__loaded_model = None
    
    def get_latest_model_path(self):

        
        try:
            dir_list = os.listdir(self.model_dir)
            latest_model_dir  = dir_list[-1]
            temp_model_path = os.path.join(self.model_dir,latest_model_dir)
            model_path = os.path.join(temp_model_path,os.listdir(temp_model_path)[-1])
            return model_path
        except Exception as e:
            raise e 
    
    def get_model():
        try:
            latest_model_path = self.get_latest_model_path()
            if self.loaded_model_path != latest_model_path:
                self.__loaded_model = PipelineModel(latest_model_path)
                self.loaded_model_path = latest_model_path

                return self.__loaded_model
        except Exception as e:
            raise e
    
    def transform(self , dataframe):
        try:
            model = self.get_model()

            return model.transform(dataframe)
        except Exception as e:
            raise e 
    
class S3FinanceEstimator(FinanceComplaintEstimator, S3Estimator):

    def __init__(self, bucket_name, s3_key, region_name="ap-south-1"):
        super().__init__(bucket_name = bucket_name , region_name = region_name)
        self.key = s3_key
        self.__loaded_s3_model_path = None

    @property
    def new_latest_s3_model_path(self):
        loaded_s3_model_path = S3Estimator.get_latest_model_path(self, self.key)

        return loaded_s3_model_path
    
    def get_latest_model_path(self):
        s3_latest_model_path = self.new_latest_s3_model_path
        if self.__loaded_s3_model_path != s3_latest_model_path:
            self.load(key=s3_latest_model_path, extract_dir=self.model_dir)
        return FinanceComplaintEstimator.get_latest_model_path(self)

        









        




    
    

    



        
