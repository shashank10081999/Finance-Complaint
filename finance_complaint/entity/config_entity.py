from collections import namedtuple

TrainingPipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])

DataIngestionConfig = namedtuple("Data_ingestion" , ["form_date" , "to_data" , "data_ingestion_dir" , "download_dir" , "file_name" , "feature_store_dir" , "failed_dir","metadata_file_path","datasource_url"])
