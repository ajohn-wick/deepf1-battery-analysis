# -----------------------------------------------------------
# Lambda Function to List RAW Battery (MATLAB) Files Stored into the RAW layer of the Data Lake
# -----------------------------------------------------------

import os
import boto3
import botocore
from libs.utils import list_objects_by_prefix

# import json
import logging

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

##### ENVIRONMENT VARIABLES #####
S3_RAW_BUCKET_NAME = os.getenv("S3_RAW_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX")
##### END ENVIRONMENT VARIABLES #####


def handler(event, context):
    # logger.info('Received event: ' + json.dumps(event, indent=2))
    if S3_RAW_BUCKET_NAME != "" and S3_PREFIX != "":
        try:
            s3_prefix_aux = S3_PREFIX
            if s3_prefix_aux[-1] != "/":
                s3_prefix_aux += "/"
            raw_battery_files = list_objects_by_prefix(
                S3_RAW_BUCKET_NAME, s3_prefix_aux
            )
            raw_battery_files_aux = []
            for s3_file_key in raw_battery_files:
                raw_battery_files_aux.append({"BATTERY_FILE_KEY": s3_file_key})
            return {"RAW_BATTERY_FILES": raw_battery_files_aux}
        except Exception as e:
            logger.exception(e, exc_info=False)
            raise e
