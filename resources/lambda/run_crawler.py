# -----------------------------------------------------------
# Lambda Function to Run a Glue Crawler in order to obtain a Data Catalog definition of data parsed via the Crawler
# -----------------------------------------------------------

import os

# import json
from datetime import date, datetime
import boto3
import botocore
import logging

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

##### ENVIRONMENT VARIABLES #####
CRAWLER_NAME = os.getenv("CRAWLER_NAME")
##### END ENVIRONMENT VARIABLES #####

glue_client = boto3.client("glue")


def handler(event, context):
    class CrawlerThrottlingException(Exception):
        pass

    class CrawlerRunningException(Exception):
        pass

    try:
        # Get Glue crawler name
        # logger.info("CrawlerName: %s", CRAWLER_NAME)
        response = glue_client.start_crawler(Name=CRAWLER_NAME)
        # logger.info('Response: %s', json.dumps(response))

        return {"StatusCode": response["ResponseMetadata"]["HTTPStatusCode"]}

    except botocore.exceptions.ClientError as e:
        logger.exception(e, exc_info=False)

        if e.response.get("Error", {}).get("Code") == "ThrottlingException":
            raise CrawlerThrottlingException(e)
        elif e.response.get("Error", {}).get("Code") == "CrawlerRunningException":
            raise CrawlerRunningException(e)
        else:
            raise e

    except Exception as e:
        logger.exception(e, exc_info=False)
        raise e
