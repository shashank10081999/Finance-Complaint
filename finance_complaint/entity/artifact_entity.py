from collections import namedtuple
from datetime import datetime

DataIngestionArtifact = namedtuple("DataIngestionArtifact",
                                   ["feature_store_file_path", "metadata_file_path", "download_dir"])

DataValidationArtifact = namedtuple("DataValidationArtifact" , ["accpeted_file_path" , "rejected_dir"])

DataTransformationArtifact = namedtuple("DataTransformationArtifact" , ["export_pipeline_file_path" , "transformed_train_file_path" , "transformed_test_file_path"])


