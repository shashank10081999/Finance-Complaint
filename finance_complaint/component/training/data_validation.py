import os
import sys
from collections import namedtuple
from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from finance_complaint.config.spark_manager import spark_session
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from finance_complaint.entity.config_entity import DataValidationConfig
from finance_complaint.entity.schema import FinanceDataSchema
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger

from pyspark.sql.functions import lit
from finance_complaint.entity.artifact_entity import DataValidationArtifact

COMPLAINT_TABLE = "complaint"
ERROR_MESSAGE = "error_msg"
MissingReport = namedtuple("MissingReport", ["total_row", "missing_row", "missing_percentage"])

class DataValidation():

    def __init__(self,data_validation_config:DataValidationConfig, 
                    data_ingestion_artifact:DataIngestionArtifact ,
                    table_name: str = COMPLAINT_TABLE, 
                    schema:FinanceDataSchema()):
        try:
            super().__init__()
            self.data_validation_config : DataValidationConfig = data_validation_config
            self.data_ingestion_artifact:DataIngestionArtifact = data_ingestion_artifact
            self.table_name = table_name
            self.schema = schema
        except Exception as e:
            raise e
    
    def read_data(self):

        try:

        dataframe = spark_session.read.parquet(self.data_ingestion_artifact.feature_store_file_path).limit(10000)

        return dataframe

        except Exception as e:
            raise e
    
    def 


