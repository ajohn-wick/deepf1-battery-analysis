# -----------------------------------------------------------
# Lambda Function to Load MATLAB Battery Data Sets into the RAW layer of the Data Lake
# -----------------------------------------------------------

import os
import boto3
import botocore

# import json
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import logging

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

##### ENVIRONMENT VARIABLES #####
DATASETS_ENDPOINT = os.getenv("DATASETS_ENDPOINT")
S3_RAW_BUCKET_NAME = os.getenv("S3_RAW_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX")
##### END ENVIRONMENT VARIABLES #####

s3_client = boto3.client("s3")


def handler(event, context):
    # logger.info('Received event: ' + json.dumps(event, indent=2))
    if (
        "DATASET_NAME" in event
        and event["DATASET_NAME"] != ""
        and DATASETS_ENDPOINT != ""
        and S3_RAW_BUCKET_NAME != ""
        and S3_PREFIX != ""
    ):
        try:
            s3_prefix_aux = S3_PREFIX
            if s3_prefix_aux[-1] != "/":
                s3_prefix_aux += "/"
            zip_archive = event["DATASET_NAME"]
            zip_archive.replace(" ", "")
            if zip_archive[-4:] == ".zip":
                # logger.info(zip_archive)
                zipped_files = urlopen(DATASETS_ENDPOINT + zip_archive)
                raw_unzipped_files = ZipFile(BytesIO(zipped_files.read()))
                # Remove README txt file(s) from the zip archive
                expected_unzipped_files = [
                    file_name
                    for file_name in raw_unzipped_files.namelist()
                    if file_name.find("README") == -1
                ]
                # logger.info(expected_unzipped_files)
                for matlab_file in expected_unzipped_files:
                    if matlab_file != "":
                        matlab_file_key = (
                            s3_prefix_aux
                            + zip_archive[:-4].lower().replace("-", "_")
                            + "/"
                            + matlab_file
                        )
                        s3_client.put_object(
                            Body=raw_unzipped_files.read(matlab_file),
                            Bucket=S3_RAW_BUCKET_NAME,
                            Key=matlab_file_key,
                        )
        except Exception as e:
            logger.exception(e, exc_info=False)
            raise e
