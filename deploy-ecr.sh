#!/bin/bash
#
# Script to deply a container image to an AWS Lambda Function
#
# Command line arguments:
# [1] registry: Registry URI
# [2] repository: Name of repository to create
# 
# Example usage: ./delpoy-ecr.sh "account-id.dkr.ecr.region.amazonaws.com" "my-lambda-container"

REGISTRY=$1
REPOSITORY=$2

# Determine if repo exists
response=$(aws ecr describe-repositories --repository-names "$REPOSITORY" 2>&1)
repo=$(echo "$response" | jq '.repositories[0].repositoryName')
repo="${repo%\"}"    # Remove suffix double quote
repo="${repo#\"}"    # Remove prefix double quote

if [[ "$repo" == "$REPOSITORY" ]]; then
    echo "Repository exists: '$REPOSITORY' and will not be created."
else
    # Creat repo
    echo "Respository does not exist. Creating repository: $REPOSITORY."
    response=$(aws ecr create-repository --repository-name "$REPOSITORY" \
                --image-tag-mutability "MUTABLE" \
                --image-scanning-configuration scanOnPush=false \
                --encryption-configuration encryptionType="AES256" )
    
    # Test if repo was created
    status=$(echo "$response" | jq '.repository.repositoryName')
    status="${status%\"}"    # Remove suffix double quote
    status="${status#\"}"    # Remove prefix double quote
    if [[ "$status" == "$REPOSITORY" ]]; then
        echo "Repository was created."
    else
        echo "Respository could not be created."
    fi
fi
