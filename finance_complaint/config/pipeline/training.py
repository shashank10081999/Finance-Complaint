import os , sys
from datetime import datetime
from finance_complaint.constant.training_pipeline_config import PIPELINE_ARTIFACT_DIR , PIPELINE_NAME
from finance_complaint.constant.training_pipeline_config.data_ingestion import *
from finance_complaint.constant.training_pipeline_config.data_validation import *
from finance_complaint.constant.training_pipeline_config.data_transformation import *
from finance_complaint.constant.training_pipeline_config.model_trainer import *
from finance_complaint.entity.metadata_entity import DataIngestionMetadata
from finance_complaint.constant import TIMESTAMP
from finance_complaint.entity.config_entity import DataIngestionConfig , TrainingPipelineConfig ,DataValidationConfig , DataTransformationConfig,ModelTrainerConfig



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
    def get_data_ingestion_config(self,form_date=DATA_INGESTION_MIN_START_DATE, to_date=None) -> DataIngestionConfig:
        try:
            min_start_date = datetime.strptime(DATA_INGESTION_MIN_START_DATE, "%Y-%m-%d")
            form_date_obj = datetime.strptime(form_date, "%Y-%m-%d")
            if form_date_obj < min_start_date:
                form_date = DATA_INGESTION_MIN_START_DATE
            
            if to_date is None:
                to_date = datetime.now().strftime("%Y-%m-%d")
            
            #if from_date > to_date:
            #    raise Exception("From Date is larger than To Date")
            
            data_ingestion_master_dir = os.path.join(self.pipeline_config.artifact_dir,DATA_INGESTION_DIR)

            data_ingestion_dir = os.path.join(data_ingestion_master_dir,self.timestamp)

            metadata_file_path = os.path.join(data_ingestion_master_dir,DATA_INGESTION_METADATA_FILE_NAME)

            data_ingestion_metadata = DataIngestionMetadata(metadata_file_path=metadata_file_path)

            if data_ingestion_metadata.is_metadata_file_present:
                metadata_info = data_ingestion_metadata.get_metadata_info()
                form_date = metadata_info.to_date
            
            data_ingestion_download_dir = os.path.join(data_ingestion_dir,DATA_INGESTION_DOWNLOAD_DATA_DIR)

            data_ingestion_feature_store_dir = os.path.join(data_ingestion_master_dir,DATA_INGESTION_FEATURE_STORE_DIR)

            data_ingestion_failed_dir = os.path.join(data_ingestion_dir,DATA_INGESTION_FAILED_DIR)

            data_ingestion_config = DataIngestionConfig(form_date=form_date , 
                                                            to_date=to_date,
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

    def get_data_validation_config(self) -> DataValidationConfig:

        try:
            data_validation_path = os.path.join(self.training_pipeline_config.artifact_dir,DATA_VALIDATION_DIR , self.timestamp)

            data_validation_accepted_file_path = os.path.join(data_validation_path,DATA_VALIDATION_ACCEPTED_DATA_DIR)
            data_validation_rejected_dir = os.path.join(data_validation_path,DATA_VALIDATION_REJECTED_DATA_DIR)

            data_validation_config = DataValidationConfig(accepted_data_dir = data_validation_accepted_file_path ,
                                                            rejected_data_dir = data_validation_rejected_dir,
                                                            file_name = DATA_VALIDATION_FILE_NAME)

            return data_validation_config
        except Exception as e:
            raise e

    def get_transformation_config(self) -> DataTransformationConfig:
        
        try:
            data_transformation_dir = os.path.join(self.training_pipeline_config.artifact_dir,DATA_TRANSFORMATION_DIR , self.timestamp)
            transformated_export_pipeline_dir = os.path.join(data_transformation_dir,DATA_TRANSFORMATION_PIPELINE_DIR)
            transformated_train_dir = os.path.join(data_transformation_dir,DATA_TRANSFORMATION_TRAIN_DIR)
            transformated_test_dir = os.path.join(data_transformation_dir,DATA_TRANSFORMATION_TEST_DIR)
            test_size = DATA_TRANSFORMATION_TEST_SIZE
            transformated_file_name = DATA_TRANSFORMATION_FILE_NAME

            data_transformation_config = DataTransformationConfig(file_name = transformated_file_name , 
                                                                export_pipeline_dir = transformated_export_pipeline_dir,
                                                                transformated_train_dir = transformated_train_dir,
                                                                transformated_test_dir = transformated_test_dir,
                                                                test_size = test_size)

            return data_transformation_config
        except Exception as e:
            raise e

    def get_model_trainer_config(self) -> ModelTrainerConfig:
        try:
            model_trainer_dir = os.path.join(self.pipeline_config.artifact_dir , MODEL_TRAINER_DIR , self.timestamp)

            trained_model_file_path = os.path.join(model_trainer_dir , MODEL_TRAINER_TRAINED_MODEL_DIR , MODEL_TRAINER_MODEL_NAME)

            label_indexer_model_dir = os.path.join(model_trainer_dir , MODEL_TRAINER_LABEL_INDEXER_DIR)

            model_trainer_config = ModelTrainerConfig(base_accuracy = MODEL_TRAINER_BASE_ACCURACY ,
                                                        trained_model_file_path = trained_model_file_path,
                                                        metric_list = MODEL_TRAINER_MODEL_METRIC_NAMES,
                                                        label_indexer_model_dir = label_indexer_model_dir)
            
            return model_trainer_config
            
        except Exception as e:
            raise e 





        
        
            

        




            






