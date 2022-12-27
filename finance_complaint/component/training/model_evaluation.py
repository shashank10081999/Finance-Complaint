from finance_complaint.entity.artifact_entity import ModelEvaluationArtifact, DataValidationArtifact, \
    ModelTrainerArtifact
from finance_complaint.entity.config_entity import ModelEvaluationConfig
from finance_complaint.entity.schema import FinanceDataSchema
import sys
from pyspark.sql import DataFrame
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.pipeline import PipelineModel
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.utils import get_score

from pyspark.sql.types import StringType, FloatType, StructType, StructField
from finance_complaint.entity.estimator import S3FinanceEstimator


class ModelEvaluation():

    def __init__(self,model_trainer_artifact,model_evaluation_config , data_validation_artifact,schema=FinanceDataSchema()):
        
        try:
            self.data_validation_artifact = data_validation_artifact
            self.model_trainer_artifact = model_trainer_artifact
            self.model_evaluation_cofig = model_evaluation_cofig
            self.schema = schema
            self.bucket_name = model_evaluation_config.bucket_name
            self.s3_model_dir_key = model_evaluation_config.model_dir
            self.s3_finance_estimator = S3FinanceEstimator(bucket_name= self.bucket_name,s3_key=self.s3_model_dir_key)
            
            self.metric_report_schema = StructType([StructField("model_accepted", StringType()),
                                                        StructField("changed_accuracy", FloatType()),
                                                        StructField("trained_model_path", StringType()),
                                                        StructField("best_model_path", StringType()),
                                                        StructField("active", StringType())]
                                                    )
        except Exception as e:
            raise e 
    
    def read_data(self) -> DataFrame:
        try:
            file_path = self.data_validation_artifact.accpeted_file_path
            dataframe = spark_session.read.parquet(file_path)
            return dataframe
        except Exception as e:
            raise e
    
    def evaluate_trained_model(self) -> ModelEvaluationArtifact:
        model_accepted , is_active = False,False

        trained_model_file_path = self.model_trainer_artifact.model_trainer_ref_artifact.trained_model_file_path
        label_indexer_model_path = self.model_trainer_artifact.label_indexer_model_file_path

        dataframe = self.read_data()

        label_indexer_model = StringIndexerModel.load(label_indexer_model_path)
        trained_model = PipelineModel.load(trained_model_file_path)

        dataframe = label_indexer_model.transform(dataframe)

        best_model_path = self.s3_finance_estimator.get_latest_model_path()

        trained_model_dataframe = trained_model.transform(dataframe)
        beat_model_dataframe = self.s3_finance_estimator.transform(dataframe)

        trained_model_f1_score = get_score(metric = "f1", dataframe = trained_model_dataframe, 
                                            label_col = self.schema.target_indexed_label, predicted_col = self.schema.prediction_column_name)
        
        best_model_f1_score = get_score(metric = "f1", dataframe = beat_model_dataframe, 
                                            label_col = self.schema.target_indexed_label, predicted_col = self.schema.prediction_column_name)

        
        changed_accuracy = trained_model_f1_score - best_model_f1_score

        if changed_accuracy > self.model_evaluation_cofig.threshold:
            model_accepted , is_active = True , True

        model_evaluation_artifact = ModelEvaluationArtifact(model_accepted = model_accepted, changed_accuracy = changed_accuracy, 
                                                            trained_model_path = trained_model_file_path, 
                                                            best_model_path =best_model_path , 
                                                            active = is_active)
        

        return model_evaluation_artifact
    
    

    def initiate_model_evaluation(self) -> ModelEvaluationArtifact:
        try:
            model_accpeted = True 
            is_active = True 

            if not self.s3_finance_estimator.is_model_available(key = self.s3_model_dir_key):
                laest_model_file_path = None 
                trained_model_path = self.model_trainer_artifact.model_trainer_ref_artifact.trained_model_file_path
                model_evaluation_artifact = ModelEvaluationArtifact(model_accepted=model_accpeted, changed_accuracy=0.0, 
                                                                    trained_model_path = trained_model_path , best_model_path = laest_model_file_path, active = is_active)
                
            else:
                model_evaluation_artifact = self.evaluate_trained_model()

            return model_evaluation_artifact
        except Exception as e:
            raise e 

