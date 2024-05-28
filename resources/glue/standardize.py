# -----------------------------------------------------------
# ETL Job to Standardize Battery Data Sets by Reading those from the RAW Layer
# and Storing those Data Sets into the STANDARDIZED Layer of the Data Lake
# -----------------------------------------------------------

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import urllib.parse
from io import BytesIO
from scipy.io import loadmat
import numpy as np
import pandas as pd
import logging

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

###
# Init Glue Context (to be able to run the job)
##
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

###
# Retreive Required Parameters Provided to this Glue Job
##
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_RAW_BUCKET_NAME", "S3_STD_BUCKET_NAME", "BATTERY_FILE_KEY"],
)

###
# Start running the Standardize Glue Job
##
job.init(args["JOB_NAME"], args)

##### ENVIRONMENT VARIABLES #####
S3_RAW_BUCKET_NAME = args["S3_RAW_BUCKET_NAME"]
S3_STD_BUCKET_NAME = args["S3_STD_BUCKET_NAME"]
BATTERY_FILE_KEY = args["BATTERY_FILE_KEY"]
##### END ENVIRONMENT VARIABLES #####

s3_client = boto3.client("s3")
try:
    if BATTERY_FILE_KEY != "" and S3_RAW_BUCKET_NAME != "" and S3_STD_BUCKET_NAME != "":
        # Load battery data set into a NumPy array
        mlab_file_key_parsed = urllib.parse.unquote_plus(
            BATTERY_FILE_KEY, encoding="utf-8"
        )
        mlab_file = s3_client.get_object(
            Bucket=S3_RAW_BUCKET_NAME, Key=mlab_file_key_parsed
        )
        raw_battery_dataset = loadmat(
            BytesIO(mlab_file["Body"].read()), squeeze_me=True, simplify_cells=True
        )
        mlab_battery_key = BATTERY_FILE_KEY.split("/")[2][:-4]
        cycle_struct = raw_battery_dataset[mlab_battery_key]["cycle"]
        # logger.info(cycle_struct)
        # List of operations present/known as of today in the dataset
        # Could be improved by storing those in a key/pair (NoSQL) table to manage operations/columns dynamically
        operation_types = {
            "charge": [
                "Voltage_measured",
                "Current_measured",
                "Temperature_measured",
                "Current_charge",
                "Voltage_charge",
                "Current_load",
                "Voltage_load",
                "Time",
            ],
            "discharge": [
                "Voltage_measured",
                "Current_measured",
                "Temperature_measured",
                "Current_charge",
                "Voltage_charge",
                "Current_load",
                "Voltage_load",
                "Time",
                "Capacity",
            ],
            "impedance": [
                "Sense_current",
                "Battery_current",
                "Current_ratio",
                "Battery_impedance",
                "Rectified_Impedance",
                "Re",
                "Rct",
            ],
        }

        standardized_battery_dataset = []
        for cycle in cycle_struct:
            operation_columns = operation_types.get(cycle["type"], "unknown")
            # Ensure operations are the ones present/known as of today in the dataset
            if operation_columns != "unknown":
                dict_battery_dataset = {}
                dict_battery_dataset["battery"] = mlab_battery_key
                dict_battery_dataset["type"] = cycle["type"]
                dict_battery_dataset["ambient_temperature"] = cycle[
                    "ambient_temperature"
                ]
                # logger.info(dict_battery_dataset)

                # Convert MATLAB date vector format into a human-readable date format
                dict_battery_dataset["datetime"] = []
                for time in cycle["time"]:
                    timestamps = pd.to_datetime(time, unit="D")
                    dict_battery_dataset["datetime"].append(
                        timestamps.strftime("%Y-%m-%d %H:%M:%S.%f")
                    )
                # logger.info(dict_battery_dataset)

                # Loop on the data structure containing measurements in order to get a standardized battery dataset
                op_data = cycle["data"]
                for op_data_col in operation_columns:
                    if op_data_col in op_data:
                        if isinstance(op_data[op_data_col], np.ndarray):
                            dict_battery_dataset[op_data_col] = op_data[
                                op_data_col
                            ].tolist()
                        else:
                            dict_battery_dataset[op_data_col] = op_data[op_data_col]
                # logger.info(dict_battery_dataset)
                standardized_battery_dataset.append(dict_battery_dataset)

        # logger.info(standardized_battery_dataset[0])
        battery_df = pd.DataFrame(standardized_battery_dataset).astype(str)
        # Remove Duplicated Columns from Standardized Data Set
        battery_df = battery_df.loc[:, ~battery_df.columns.duplicated()]
        # logger.info(battery_df.head(1))
        battery_df.to_parquet(
            f"s3://{S3_STD_BUCKET_NAME}/battery/",
            partition_cols=["battery"],
            compression="snappy",
            index=False,
        )

except Exception as e:
    logger.exception(e, exc_info=False)
    raise e

job.commit()
