import os
import argparse
from finance_complaint.pipeline.training import TrainingPipeline
from finance_complaint.config.pipeline.training import FinanceConfig
import sys


def start_training(start=False):
    try:
        if not start:
            return None 
        TrainingPipeline(FinanceConfig()).start()
    except Exception as e:
        raise e 

def main(training_status):
    
    try:
           start_training(training_status)
    except Exception as e:
        raise e

if __name__ == "__main__":
    training_status = True 
    main(training_status)
