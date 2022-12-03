import os , sys
import datetime
from finance_complaint.constant.training_pipeline_config import PIPELINE_ARTIFACT_DIR , PIPELINE_NAME
from finance_complaint.constant.training_pipeline_config.data_ingestion import DATA_INGESTION_FILE_NAME , DATA_INGESTION_DATA_SOURCE_URL , DATA_INGESTION_FAILED_DIR , DATA_INGESTION_MIN_START_DATE , DATA_INGESTION_DIR,DATA_INGESTION_METADATA_FILE_NAME,DATA_INGESTION_DOWNLOAD_DATA_DIR,DATA_INGESTION_FEATURE_STORE_DIR
from finance_complaint.entity.metadata_entity import DataIngestionMetadata
from finance_complaint.constant import TIMESTAMP
from finance_complaint.entity.config_entity import Data_ingestion_config , TrainingPipelineConfig



class FinanceConfig():

    def __init__(self,pipeline_name=PIPELINE_NAME, timestamp=TIMESTAMP):
        self.timestamp = timestamp
        self.pipeline_name = pipeline_name
        self.pipeline_config = self.get_pipeline_config()

    def get_pipeline_config(self) -> TrainingPipelineConfig:
        try:
            artifact_dir = PIPELINE_ARTIFACT_DIR
            pipeline_config = TrainingPipelineConfig(self.pipeline_name,artifact_dir)
            return pipeline_config
        except Exception as e:
            raise e
    def get_data_ingestion_config(self,from_date=DATA_INGESTION_MIN_START_DATE, to_date=None) -> Data_ingestion_config:
        try:
            from_date = datetime.strptime(from_date, "%Y-%m-%d")
            if from_date > to_date:
                raise Exception("From Date is larger than To Date")
            
            if to_date is not None:
                to_date = datetime.now().strptime( "%Y-%m-%d")
            
            data_ingestion_master_dir = os.path.join(self.pipeline_config.artifact_dir,DATA_INGESTION_DIR)

            data_ingestion_dir = os.path.join(data_ingestion_master_dir,self.timestamp)

            metadata_file_path = os.path.join(data_ingestion_master_dir,DATA_INGESTION_METADATA_FILE_NAME)

            data_ingestion_metadata = DataIngestionMetadata(metadata_file_path=metadata_file_path)

            if data_ingestion_metadata.is_metadata_file_present():
                metadata_info = data_ingestion_metadata.get_metadata_info()
                from_date = metadata_info.to_date
            
            data_ingestion_download_dir = os.path.join(data_ingestion_dir,DATA_INGESTION_DOWNLOAD_DATA_DIR)

            data_ingestion_feature_store_dir = os.path.join(data_ingestion_master_dir,DATA_INGESTION_FEATURE_STORE_DIR)

            data_ingestion_failed_dir = os.path.join(data_ingestion_dir,DATA_INGESTION_FAILED_DIR)

            data_ingestion_config = Data_ingestion_config(form_date=from_date , 
                                                            to_data=to_date,
                                                            data_ingestion_dir= data_ingestion_dir,
                                                            download_dir= data_ingestion_download_dir,
                                                            file_name=DATA_INGESTION_FILE_NAME,
                                                            feature_store_dir=data_ingestion_feature_store_dir,
                                                            failed_dir=data_ingestion_failed_dir,
                                                            metadata_file_path=metadata_file_path,
                                                            datasource_url=DATA_INGESTION_DATA_SOURCE_URL)
            return data_ingestion_config

        except Exception as e:
            raise e 


            






