#!/bin/bash
#
# Script to deploy a container image to an AWS Lambda Function
#
# Command line arguments:
# [1] function_name: Name of AWS Lambda function name
# [2] image_uri: URI of container of image
# 
# Example usage: ./delpoy-lambda.sh "my-lambda-function" "account-id.dkr.ecr.region.amazonaws.com/my-lambda-container:tag"

FUNCTION_NAME=$1
IMAGE_URI=$2

response=$(aws lambda update-function-code --function-name $FUNCTION_NAME --image-uri $IMAGE_URI)

aws lambda wait function-updated-v2 --function-name $FUNCTION_NAME

echo "Container image has been deployed to Lambda."
