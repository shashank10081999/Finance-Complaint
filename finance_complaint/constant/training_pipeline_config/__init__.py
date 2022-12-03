import os,sys
from finance_complaint.constant.training_pipeline_config.data_ingestion import *


PIPELINE_NAME = "finance-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "finance_artifact")