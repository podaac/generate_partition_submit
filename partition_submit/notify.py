#!/app/env/bin/python3
"""Logs and sends email notification when processor encounters an error.

Logs the error message.
Pusblishes error message to SNS Topic.
"""

# Standard imports
import os
import sys

# Third-party imports
import boto3
import botocore

# Constants
TOPIC_STRING = "batch-job-failure"

def notify(logger, sigevent_type, sigevent_description, sigevent_data=""):
    """Handles error events."""
    
    publish_event(sigevent_type, sigevent_description, sigevent_data, logger)
    
def publish_event(sigevent_type, sigevent_description, sigevent_data, logger):
    """Publish event to SNS Topic."""
    
    sns = boto3.client("sns")
    
    # Get topic ARN
    try:
        topics = sns.list_topics()
    except botocore.exceptions.ClientError as e:
        logger.error("Failed to list SNS Topics.")
        logger.error(f"Error - {e}")
        sys.exit(1)
    for topic in topics["Topics"]:
        if TOPIC_STRING in topic["TopicArn"]:
            topic_arn = topic["TopicArn"]
            
    # Publish to topic
    subject = f"Generate Partition & Submit Lambda Failure"
    message = f"The Generate Partition & Submit component has encountered an error.\n" \
        + f"Log file: {os.getenv('AWS_LAMBDA_LOG_GROUP_NAME')}/{os.getenv('AWS_LAMBDA_LOG_STREAM_NAME')}.\n" \
        + f"Error type: {sigevent_type}.\n" \
        + f"Error description: {sigevent_description}\n"
    if sigevent_data != "": message += f"Error data: {sigevent_data}"
    try:
        response = sns.publish(
            TopicArn = topic_arn,
            Message = message,
            Subject = subject
        )
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to publish to SNS Topic: {topic_arn}.")
        logger.error(f"Error - {e}")
        sys.exit(1)
    
    logger.info(f"Message published to SNS Topic: {topic_arn}.")