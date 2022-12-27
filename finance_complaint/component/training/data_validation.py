import os
import sys
from collections import namedtuple
from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from finance_complaint.entity.config_entity import DataValidationConfig
from finance_complaint.entity.schema import FinanceDataSchema

from pyspark.sql.functions import lit
from finance_complaint.entity.artifact_entity import DataValidationArtifact

COMPLAINT_TABLE = "complaint"
ERROR_MESSAGE = "error_msg"
MissingReport = namedtuple("MissingReport", ["total_row", "missing_row", "missing_percentage"])

class DataValidation():

    def __init__(self,data_validation_config:DataValidationConfig,
                data_ingestion_artifact:DataIngestionArtifact, 
                schema = FinanceDataSchema() ,table_name: str = COMPLAINT_TABLE):
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
    
    def get_missing_report(self,dataframe):
        try:
            missing_report = dict()
            number_of_rows = dataframe.count()

            for i in dataframe.columns:
                missing_row = dataframe.filter(f"{i} is null").count()
                missing_percentage = (missing_row / number_of_rows)*100
                missing_report[i] = MissingReport(total_row=number_of_rows , missing_row = missing_row , missing_percentage = missing_percentage)

            return missing_report
        except Exception as e:
            raise e
    
    def get_unwanted_and_high_missing_value_columns(self,dataframe,threshold:float = 0.2):
        try:
            missing_report = self.get_missing_report(dataframe)

            unwanted_column: List[str] = self.schema.unwanted_columns

            for column in missing_report:
                if missing_report[column].missing_percentage < threshold:
                    unwanted_column.append(column)

            print(unwanted_column)
            for i in list(set(unwanted_column)):
                if i in self.schema.required_columns:
                    unwanted_column.remove(i)
            
            print(list(set(unwanted_column)))
            
            return list(set(unwanted_column))
        except Exception as e:
            raise e
    
    def drop_unwanted_columns(self, dataframe: DataFrame) -> DataFrame:
        try:
            unwanted_columns = self.get_unwanted_and_high_missing_value_columns(dataframe)
            unwanted_dataframe = dataframe.select(unwanted_columns)
            unwanted_dataframe = unwanted_dataframe.withColumn("Error_message", lit("column has many missing values"))

            rejected_dir = os.path.join(self.data_validation_config.rejected_data_dir,"missing_values_dir")

            os.makedirs(rejected_dir,exist_ok=True)

            file_path = os.path.join(rejected_dir,self.data_validation_config.file_name)

            unwanted_dataframe.write.mode("append").parquet(file_path)

            dataframe: DataFrame = dataframe.drop(*unwanted_columns)

            return dataframe
        except Exception as e:
                    raise e



    def get_unique_values_of_each_column(self,dataframe) -> None:
        try:
            for i in dataframe.columns:
                n_unique: int = dataframe.select(col(column)).distinct().count()
                n_missing: int = dataframe.filter(col(column).isNull()).count()
                missing_percentage = (n_missing / dataframe.count())*100
                print(f"Column: {i} contains {n_unique} value and missing perc: {missing_percentage} %.")
        except Exception as e:
            raise e

    def is_required_columns_exist(self,dataframe) -> None:

        try:
            required_columns = self.schema.required_columns

            for i in required_columns:
                if i not in dataframe.columns:
                    #raise Exception(f"Column: {i} is a imp column and its missing for the dataframe , Please check the issuse")
                    print(f"Column {i} is not present in the dataframe ")

        except Exception as e:
            raise e

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            print("Strating the Data Validation")
            dataframe : DataFrame = self.read_data()

            dataframe : DataFrame = self.drop_unwanted_columns(dataframe)

            self.is_required_columns_exist(dataframe)

            os.makedirs(self.data_validation_config.accepted_data_dir,exist_ok=True)

            accepted_file_path = os.path.join(self.data_validation_config.accepted_data_dir,self.data_validation_config.file_name)

            dataframe.write.parquet(accepted_file_path)

            data_validation_artifact = DataValidationArtifact(accpeted_file_path = accepted_file_path , rejected_dir=self.data_validation_config.rejected_data_dir)

            return data_validation_artifact
        except Exception as e:
            raise e

    
    





