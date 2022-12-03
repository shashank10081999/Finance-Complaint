import os , sys 
import yaml



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
        with open(file_path, "r") as yaml_file:
            return yaml.load(yaml_file)
    except Exception as e:
        raise e