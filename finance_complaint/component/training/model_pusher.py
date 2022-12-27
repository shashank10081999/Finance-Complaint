from statistics import mode
import sys
from finance_complaint.entity.config_entity import ModelPusherConfig
from finance_complaint.entity.artifact_entity import ModelPusherArtifact, ModelTrainerArtifact
from pyspark.ml.pipeline import PipelineModel
from finance_complaint.entity.estimator import S3FinanceEstimator
import os


class ModelPusher():

    def __init__(self, model_pusher_config: ModelPusherConfig , model_trainer_artifact: ModelTrainerArtifact):
        self.model_pusher_config = model_pusher_config
        self.model_trainer_artifact = model_trainer_artifact
    
    def push_model(self) -> str():
        try:
            model_registry = S3FinanceEstimator(bucket_name = self.model_pusher_config.bucket_name, s3_key = self.model_pusher_config.model_dir)
            model_file_path = self.model_trainer_artifact.model_trainer_ref_artifact.trained_model_file_path
            model_registry.save(model_dir = os.path.dirname(model_file_path), key = self.model_pusher_config.model_dir)
            return model_registry.get_latest_model_path()
        except Exception as e:
            raise e 
        

    def initiate_model_pusher(self) -> ModelPusherArtifact:
        try:
            push_dir = self.push_model()
            model_pusher_artifact = ModelPusherArtifact(model_pushed_dir=pushed_dir)

            return model_pusher_artifact
        except Exception as e:
            raise e