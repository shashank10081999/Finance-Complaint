from collections import namedtuple
from datetime import datetime

DataIngestionArtifact = namedtuple("DataIngestionArtifact",
                                   ["feature_store_file_path", "metadata_file_path", "download_dir"])

DataValidationArtifact = namedtuple("DataValidationArtifact" , ["accpeted_file_path" , "rejected_dir"])

DataTransformationArtifact = namedtuple("DataTransformationArtifact" , ["export_pipeline_file_path" , "transformed_train_file_path" , "transformed_test_file_path"])

PartialModelTrainerRefArtifact = namedtuple("PartialModelTrainerRefArtifact", ["trained_model_file_path",
                                                                               "label_indexer_model_file_path"
                                                                               ])

PartialModelTrainerMetricArtifact = namedtuple("PartialModelTrainerMetricArtifact", [
    "f1_score", "precision_score", "recall_score"
])

class ModelTrainerArtifact():

    def __init__(self,model_trainer_ref_artifact: PartialModelTrainerRefArtifact , 
                    model_trainer_train_metric_artifact:PartialModelTrainerMetricArtifact,
                    model_trainer_test_metric_artifact:PartialModelTrainerMetricArtifact):
        self.model_trainer_ref_artifact = model_trainer_ref_artifact
        self.model_trainer_train_metric_artifact = model_trainer_train_metric_artifact
        self.model_trainer_test_metric_artifact = model_trainer_test_metric_artifact
    
    def _asdict(self):
        try:
            response = dict()
            response["model_trainer_ref_artifact"] = self.model_trainer_ref_artifact._asdict()
            response["model_trainer_train_metric_artifact"] = self.model_trainer_train_metric_artifact._asdict()
            response["model_trainer_test_metric_artifact"] = self.model_trainer_test_metric_artifact._asdict()
        except Exception as e:
            raise e 
        


