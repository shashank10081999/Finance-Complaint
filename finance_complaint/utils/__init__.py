import os , sys 
import yaml
from pyspark.ml.evaluation import MulticlassClassificationEvaluator



def write_yaml_file(file_path, data):
    try:
        os.makedirs(os.path.dirname(file_path),exist_ok = True)
        with open(file_path, "w") as yaml_file:
            if data is not None:
                yaml.dump(data,yaml_file)
    except Exception as e :
        raise e

def read_yaml_file(file_path):
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise e

def get_score(metric , dataframe , label_col , predicted_col) -> float :
    try:

        evaluator  = MulticlassClassificationEvaluator(labelCol = label_col , predictionCol = predicted_col ,
                                                        metricName = metric)
        score = evaluator.evaluate(dataframe)

        return score 

    except Exception as e:
        raise e 
