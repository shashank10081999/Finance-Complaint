import os

from finance_complaint.entity.schema import FinanceDataSchema
import sys
from pyspark.ml.feature import StringIndexer, StringIndexerModel
from pyspark.ml.pipeline import Pipeline, PipelineModel
from typing import List
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.entity.artifact_entity import DataTransformationArtifact, \
    PartialModelTrainerMetricArtifact, PartialModelTrainerRefArtifact, ModelTrainerArtifact
from finance_complaint.entity.config_entity import ModelTrainerConfig
from pyspark.sql import DataFrame
from pyspark.ml.feature import IndexToString
from pyspark.ml.classification import RandomForestClassifier
from finance_complaint.utils import get_score


class ModelTrainer():

    def __init__(self,model_trainer_config:ModelTrainerConfig,data_transformation_artifact:DataTransformationArtifact,schema=FinanceDataSchema()):
        self.model_trainer_config = model_trainer_config
        self.data_transformation_artifact = data_transformation_artifact
        self.schema = schema
    
    def get_train_test_dataframe(self) -> List[DataFrame]:
        try:
            trained_data_file_path = self.data_transformation_artifact.transformed_train_file_path
            test_data_file_path = self.data_transformation_artifact.transformed_test_file_path

            train_df = spark_session.read.parquet(trained_data_file_path)
            test_df = spark_session.read.parquet(test_data_file_path)

            dataframes = [train_df,test_df]

            return dataframes

        except Exception as e:
            raise e
    
    def get_model(self,label_indexer_model):
        try:
            stages = []

            random_forest_clf = RandomForestClassifier(featuresCol=self.schema.scaled_vector_input_features , 
                                                            labelCol=self.schema.target_indexed_label)

            label_generator = IndexToString(inputCols = self.schema.prediction_column_name,
                                            outputCol = f"{self.prediction_column_name}_{self.target_column}",
                                            labels=label_indexer_model.labels)

            stages.append(random_forest_clf)
            stages.append(label_generator)

            pipeline = Pipeline(stages=stages)

            return pipeline
        except Exception as e:
            raise e

    def get_scores(self , dataframe : DataFrame , metrics : List[str] = None):

        try:
            if metrics is None:
                metrics  = self.model_trainer_config.metric_list
            
            scores: List[tuple] = []

            for metric_name in metrics:
                score  = get_score(metric = metric , dataframe = dataframe , label_col = self.schema.target_indexed_label,
                                    predicted_col = self.schema.prediction_column_name)
                scores.appemd((metric_name , score))
            
            return scores 

        except Exception as e:
            raise e

    def export_trained_model(self , model: PipelineModel) -> PartialModelTrainerRefArtifact:

        try:

            transformed_pipeline_file_path = self.data_transformation_config.export_pipeline_file_path

            transformed_model = PipelineModel.load(transformed_pipeline_file_path)

            updated_stages = transformed_model.stages + model.stages

            transformed_model.stages = updated_stages

            trained_model_file_path = self.model_trainer_config.trained_model_file_path

            os.makedir(os.path.dirname(trained_model_file_path),exist_ok = True )

            transformed_model.save(trained_model_file_path)

            ref_artifact = PartialModelTrainerRefArtifact(
                trained_model_file_path=trained_model_file_path,
                label_indexer_model_file_path=self.model_trainer_config.label_indexer_model_dir)

            return ref_artifact

        except Exception as e:
            raise e 


    def initiate_model_training(self) -> ModelTrainerArtifact:
        try:
            dataframes = self.get_train_test_dataframe()
            train_dataframe, test_dataframe = dataframes[0], dataframes[1]

            label_indexer = StringIndexer(inputCol = self.schema.target_column , outputCol = self.schema.target_indexed_label)

            label_indexer_model = label_indexer.fit(train_dataframe)

            os.makedirs(os.path.dirname(self.model_trainer_config.label_indexer_model_dir) , exist_ok = True )

            label_indexer_model.save(self.model_trainer_config.label_indexer_model_dir)

            train_dataframe = label_indexer_model.transform(train_dataframe)
            test_dataframe = label_indexer_model.transform(test_dataframe)

            model = self.get_model(label_indexer_model)

            trained_model = model.fit(train_dataframe)

            train_dataframe_pred = trained_model.transform(train_dataframe)
            test_dataframe_pred = trained_model.transform(test_dataframe)

            train_data_scores = self.get_scores(train_dataframe_pred)

            train_metric_artifact = PartialModelTrainerMetricArtifact(f1_score=train_data_scores[0][1],
                                                                      precision_score=train_data_scores[1][1],
                                                                      recall_score=train_data_scores[2][1])

            test_data_scores = self.get_scores(test_dataframe)

            test_metric_artifact = PartialModelTrainerMetricArtifact(f1_score=test_data_scores[0][1],
                                                                      precision_score=test_data_scores[1][1],
                                                                      recall_score=test_data_scores[2][1])
            
            ref_artifact = self.export_trained_model(model = trained_model)

            model_trainer_artifact = ModelTrainerArtifact (model_trainer_ref_artifact = ref_artifact , 
                                                            model_trainer_train_metric_artifact = train_metric_artifact,
                                                            model_trainer_test_metric_artifact = test_metric_artifact)
            
            return model_trainer_artifact
        
        except Exception as e:
            raise e 

            











            



