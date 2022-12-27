from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.component.training.data_ingestion import DataIngestion
from finance_complaint.component.training.data_validation import DataValidation
from finance_complaint.component.training.data_transformation import DataTransformation
from finance_complaint.component.training.model_trainer import ModelTrainer
from finance_complaint.component.training.model_evaluation import ModelEvaluation
from finance_complaint.component.training.model_pusher import ModelPusher
from finance_complaint.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact, \
    DataTransformationArtifact, ModelTrainerArtifact, ModelEvaluationArtifact
import os , sys


class TrainingPipeline:

    def __init__(self, finance_config: FinanceConfig):
        self.finance_config: FinanceConfig = finance_config
    
    def start_data_ingestion(self):
        try:

            data_ingestion_config = self.finance_config.get_data_ingestion_config()
            data_ingestion = DataIngestion(data_ingestion_config = data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            return data_ingestion_artifact
        
        except Exception as e:
            raise e 
    
    def start_data_validation(self , data_ingestion_artifact):
        try:

            data_validation_config = self.finance_config.get_data_validation_config()
            data_validation = DataValidation(data_ingestion_artifact = data_ingestion_artifact , data_validation_config = data_validation_config)
            data_validation_artifact = data_validation.initiate_data_validation()
            return data_validation_artifact
        except Exception as e:
            raise e
    
    def start(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
        except Exception as e:
            raise e 













































































































