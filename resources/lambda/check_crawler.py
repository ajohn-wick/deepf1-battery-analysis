# -----------------------------------------------------------
# Lambda Function to Check a Glue Crawler State and return its current value
# -----------------------------------------------------------

import os

# import json
from datetime import date, datetime
import botocore
import boto3
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

    try:
        # Get Glue crawler name
        # logger.info("CrawlerName: %s", CRAWLER_NAME)
        glue_crawler = glue_client.get_crawler(Name=CRAWLER_NAME)
        # logger.info('Response: %s', response)

        crawler_info = {}
        crawler_info["CRAWLER_STATE"] = glue_crawler["Crawler"]["State"]

        if "LastCrawl" in glue_crawler["Crawler"].keys():
            crawler_info["LAST_CRAWLER_STATE"] = glue_crawler["Crawler"]["LastCrawl"][
                "Status"
            ]

        return crawler_info

    except botocore.exceptions.ClientError as e:
        logger.exception(e, exc_info=False)

        if e.response.get("Error", {}).get("Code") == "ThrottlingException":
            raise CrawlerThrottlingException(e)
        else:
            raise e

    except Exception as e:
        logger.exception(e, exc_info=False)
        raise e
