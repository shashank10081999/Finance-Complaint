import os

from finance_complaint.entity.schema import FinanceDataSchema
import sys
from pyspark.ml.feature import StandardScaler, VectorAssembler, OneHotEncoder, StringIndexer, Imputer
from pyspark.ml.pipeline import Pipeline

from finance_complaint.config.spark_manager import spark_session
from finance_complaint.entity.artifact_entity import DataValidationArtifact, DataTransformationArtifact
from finance_complaint.entity.config_entity import DataTransformationConfig
from pyspark.sql import DataFrame
from finance_complaint.ml.feature import FrequencyImputer, DerivedFeatureGenerator
from pyspark.ml.feature import IDF, Tokenizer, HashingTF
from pyspark.sql.functions import col, rand



class DataTransformation():

    def __init__(self,data_validation_artifact:DataValidationArtifact , data_transformation_config : DataTransformationConfig , schema=FinanceDataSchema()):

        
        try:
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config
            self.schema = schema
        
        except Exception as e:
            raise e

    def read_data(self) -> DataFrame:
        try:
            file_path = self.data_validation_artifact.file_path
            df = spark_session.read.parquet(file_path)
            df.printSchema()
            return df
        except Exception as e :
            raise e
    
    def get_data_transformation_pipeline(self) -> Pipeline:

        stages = []

        derived_feature = DerivedFeatureGenerator(inputClos = self.schema.derived_input_features , outputClos = self.schema.derived_output_features)

        stages.append(derived_feature)

        imputer = Imputer(inputClos = self.schema.numerical_columns , outputClos = self.schema.im_numerical_columns)

        stages.append(imputer)

        frequency_imputer = FrequencyImputer(inputClos = self.schema.one_hot_encoding_features , outputClos = self.schema.im_one_hot_encoding_features)

        stages.append(frequency_imputer)

        for im_one_hot_feature, string_indexer_col in zip(self.schema.im_one_hot_encoding_features,
                                                              self.schema.string_indexer_one_hot_features):
                string_indexer = StringIndexer(inputCol=im_one_hot_feature, outputCol=string_indexer_col)
                stages.append(string_indexer)

        one_hot_encoder = OneHotEncoder(inputClos = self.schema.string_indexer_one_hot_features ,
                                        outputClos = self.schema.tf_one_hot_encoding_features)
        
        stages.append(one_hot_encoder) 

        tokenizer = Tokenizer(self.schema.tfidf_feature[0] , outputClo = "words")

        stages.append(tokenizer)

        hashing_tf = HashingTF(inputCol = tokenizer.getOutputCols , outputClo = "raw_features" , numFeatures = 40)

        stages.append(hashing_tf)

        tf_idf = IDF(inputCol = hashing_tf.getOutputCols , outputCols = self.schema.tf_tfidf_features[0])

        stages.append(tf_idf)

        vector_assembler = VectorAssembler(inputCols=self.schema.input_features,
                                               outputCol=self.schema.vector_assembler_output)

        stages.append(vector_assembler)

        standard_scaler = StandardScaler(inputCol=self.schema.vector_assembler_output,
                                             outputCol=self.schema.scaled_vector_input_features)
        stages.append(standard_scaler)

        pipeline = Pipeline(stages = stages)

        print(pipeline)

        return pipeline




    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            print("Started the Data Transformation ")

            dataframe:DataFrame = self.read()

            test_size = self.data_transformation_config.test_size

            train_dataframe , test_dataframe = dataframe.randomsplit([1-test_size , test_size])

            pipeline = self.get_data_transformation_pipeline()

            tarnsformed_pipeline = pipeline.fit(train_dataframe)

            required_columns = [self.schema.scaled_vector_input_features, self.schema.target_column]

            transformed_trained_dataframe = tarnsformed_pipeline.transform(train_dataframe)
            transformed_trained_dataframe = transformed_trained_dataframe.select(required_columns)

            transformed_test_dataframe = tarnsformed_pipeline.transform(test_dataframe)
            transformed_test_dataframe = transformed_test_dataframe.select(required_columns)

            export_pipeline_file_path = self.data_transformation_config.export_pipeline_dir

            os.makedirs(export_pipeline_file_path , exist_ok=True )

            os.makedirs(self.data_transformation_config.transformated_train_dir , exist_ok=True)

            os.makedirs(self.data_transformation_config.transformated_test_dir , exist_ok = True)

            transformed_train_data_file_path = os.path.join(self.data_transformation_config.transformated_train_dir , self.data_transformation_config.file_name)

            transformed_test_data_file_path = os.path.join(self.data_transformation_config.transformated_test_dir , self.data_transformation_config.file_name)

            tarnsformed_pipeline.save(export_pipeline_file_path)

            transformed_trained_dataframe.write.parquet(transformed_train_data_file_path)

            transformed_test_dataframe.write.parquet(transformed_test_data_file_path)

            data_transformation_artifact = DataTransformationArtifact(
                                                                        export_pipeline_file_path = export_pipeline_file_path,
                                                                        transformed_train_file_path = transformed_train_data_file_path,
                                                                        transformed_test_file_path = transformed_test_data_file_path

                                                                        )
            
            return data_transformation_artifact
        
        except Exception as e:
            raise e









