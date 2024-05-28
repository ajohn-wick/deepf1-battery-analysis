import boto3
import botocore

# import json
import logging

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

# Method to List Objects Stored into a Specific S3 Bucket with a Particular Prefix
def list_objects_by_prefix(s3_bucket, s3_prefix="/", filter_func=None):
    next_token = ""
    s3_objects = []
    while True:
        if next_token:
            res = s3_client.list_objects_v2(
                Bucket=s3_bucket, ContinuationToken=next_token, Prefix=s3_prefix
            )
        else:
            res = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

        if "Contents" not in res:
            break

        if res["IsTruncated"]:
            next_token = res["NextContinuationToken"]
        else:
            next_token = ""

        if filter_func:
            s3_keys = [obj["Key"] for obj in res["Contents"] if filter_func(obj["Key"])]
        else:
            s3_keys = [obj["Key"] for obj in res["Contents"]]

        s3_objects.extend(s3_keys)

        if not next_token:
            break
    # logger.info("find {} files in {}".format(len(s3_objects), s3_prefix))
    # logger.info(s3_objects)
    return s3_objects
