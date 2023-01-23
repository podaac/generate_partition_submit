# -*- coding: utf-8 -*-
"""Partition & Submit: Partitions OBPG downloads into chunk-sized jobs and 
submits them as job arrays to AWS Batch.

Example command: python3 parition_and_submit_jobs.py -d /home/username/data -c /home/username/data/job_config.json -p podaac-sndbx-generate

Command line arguments:
-d, --datadir: Path to directory to store JSON and text files
-c, --config: Path to job data configuration JSON file 
-p, --prefix: Prefix for all AWS infrastructure
"""

# Standard imports
import argparse
import json
import os
import pathlib
import shutil
import sys

# Third-party imports
import boto3
import botocore

# Local imports
from partition import Partition
from submit import Submit, submit_jobs

# Constants
EFS_DIR = pathlib.Path("/mnt/data")
EFS_DIRS = {
    "downloader": "/mnt/data/downloader/lists",
    "combiner": "/mnt/data/combiner/downloads",
    "processor": "/mnt/data/processor/input"
}

def get_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Partition and submit AWS jobs")
    arg_parser.add_argument("-d",
                            "--datadir",
                            type=str,
                            help="Path to directory to store JSON and text files")
    arg_parser.add_argument("-p",
                            "--prefix",
                            type=str,
                            help="Prefix for all AWS infrastructure")
    arg_parser.add_argument("-c",
                            "--config",
                            type=str,
                            help="Path to job data configuration JSON file")
    arg_parser.add_argument("-s",
                            "--dataset",
                            type=str,
                            help="Name of dataset to produce AWS jobs for")
    arg_parser.add_argument("-l",
                            "--list",
                            type=str,
                            help="Name of download load list creator text file")
    arg_parser.add_argument("-a",
                            "--account",
                            type=str,
                            help="Identifier (integer) for AWS account")
    return arg_parser

def create_directories():
    """Creates EFS directories if they do not already exist.
    
    Typically completed for the first execution.
    """
    
    # Combiner
    combiner = EFS_DIR.joinpath("combiner")
    combiner.joinpath("downloads").mkdir(parents=True, exist_ok=True)
    combiner.joinpath("jobs").mkdir(parents=True, exist_ok=True)
    combiner.joinpath("logs").mkdir(parents=True, exist_ok=True)
    combiner.joinpath("scratch").mkdir(parents=True, exist_ok=True)
    
    # Downloader
    downloader = EFS_DIR.joinpath("downloader")
    downloader.joinpath("lists").mkdir(parents=True, exist_ok=True)
    downloader.joinpath("logs").mkdir(parents=True, exist_ok=True)
    downloader.joinpath("output").mkdir(parents=True, exist_ok=True)
    downloader.joinpath("scratch").mkdir(parents=True, exist_ok=True)
    
    # Processor
    processor = EFS_DIR.joinpath("processor")
    processor.joinpath("input").mkdir(parents=True, exist_ok=True)
    processor.joinpath("logs").mkdir(parents=True, exist_ok=True)
    processor.joinpath("output", "MODIS_L2P_CORE_NETCDF").mkdir(parents=True, exist_ok=True)
    processor.joinpath("output", "VIIRS_L2P_CORE_NETCDF").mkdir(parents=True, exist_ok=True)
    processor.joinpath("scratch", "current_jobs").mkdir(parents=True, exist_ok=True)
    processor.joinpath("scratch", "current_logs").mkdir(parents=True, exist_ok=True)
    processor.joinpath("scratch", "email").mkdir(parents=True, exist_ok=True)
    processor.joinpath("scratch", "locks").mkdir(parents=True, exist_ok=True)
    processor.joinpath("scratch", "quarantine").mkdir(parents=True, exist_ok=True)    

def copy_to_efs(datadir, partitions):
    """Copy DLC text files and coordination JSON files to EFS.
    
    Assumption: The number of JSON files is same for all components
    """
    
    # Create EFS directories if they don't exist
    create_directories()
    
    ptypes = []
    if "quicklook" in partitions.keys(): ptypes.append("quicklook")
    if "refined" in partitions.keys(): ptypes.append("refined")
    if "unmatched" in partitions.keys(): ptypes.append("unmatched")
    
    for ptype in ptypes:
        # Copy text files to downloader directory
        txts = [ txt_file for txt_list in partitions[ptype]["downloader_txt"] for txt_file in txt_list  ]
        for txt in txts:
            shutil.copyfile(f"{datadir}/{txt}", f"{EFS_DIRS['downloader']}/{txt}")
    
        # Copy JSON files to appropriate directories 
        for i in range(len(partitions[ptype]["combiner"])):
            shutil.copyfile(f"{datadir}/{partitions[ptype]['downloader'][i]}", f"{EFS_DIRS['downloader']}/{partitions[ptype]['downloader'][i]}")
            if ptype == "unmatched": continue
            shutil.copyfile(f"{datadir}/{partitions[ptype]['combiner'][i]}", f"{EFS_DIRS['combiner']}/{partitions[ptype]['combiner'][i]}")
            shutil.copyfile(f"{datadir}/{partitions[ptype]['processor'][i]}", f"{EFS_DIRS['processor']}/{partitions[ptype]['processor'][i]}")  
            
def delete_s3(dataset, prefix, downloads_list):
    """Delete DLC-created download lists from S3 bucket."""
    
    s3 = boto3.client("s3")
    for txt_file in downloads_list:
        try:
            response = s3.delete_object(Bucket=f"{prefix}-download-lists",
                                        Key=f"{dataset}/{txt_file}")
            print(f"S3 file deleted: {dataset}/{txt_file}")      
        except botocore.exceptions.ClientError as e:
            raise e
            

def handle_error(error):
    """Print out error message and exit."""
    
    print("Error encountered.")
    print(error)
    print("System exiting.")
    sys.exit(1)
    
def print_jobs(partitions):
    """Print the number of jobs per component and processing type."""
    
    if "quicklook" in partitions.keys():
        print(f"Number of quicklook downloader jobs: {len(partitions['quicklook']['downloader'])}")
        print(f"Number of quicklook combiner jobs: {len(partitions['quicklook']['combiner'])}")
        print(f"Number of quicklook processor jobs: {len(partitions['quicklook']['processor'])}")
        
    if "refined" in partitions.keys():
        print(f"Number of refined downloader jobs: {len(partitions['refined']['downloader'])}")
        print(f"Number of refined combiner jobs: {len(partitions['refined']['combiner'])}")
        print(f"Number of refined processor jobs: {len(partitions['refined']['processor'])}")
    
    if "unmatched" in partitions.keys():
        print(f"Number of unmatched downloader jobs: {len(partitions['unmatched']['downloader'])}")

def event_handler(event, context):
    """AWS Lambda event handler that kicks off partition of data and submits
    AWS Batch jobs."""
    
    # Arguments - support local and AWS execution
    if not os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        arg_parser = get_args()
        args = arg_parser.parse_args()
        account = args.account
        region = "us-west-2"
        dataset = args.dataset
        datadir = args.datadir
        prefix = args.prefix
        config = args.config
        download_lists = args.list
    else:
        account = event["Records"][0]["eventSourceARN"].split(':')[4]
        region = event["Records"][0]["awsRegion"]
        body = event["Records"][0]["body"].replace("'", '"')
        body = json.loads(body)
        dataset = body["dataset"]
        datadir = "/tmp"
        prefix = body["prefix"]
        config = "job_config.json"
        download_lists = body["txt_list"]
    
    # Partition
    try:
        partition = Partition(dataset, download_lists, datadir, prefix)
        partition.num_lic_avail = 5
        partitions = partition.partition_downloads(region, account, prefix)
        print(f"Number of licenses available: {partition.num_lic_avail}.")
    except botocore.exceptions.ClientError as e:
        handle_error(e)
    
    # If there are downloads and available licenses, then submit jobs
    if partitions:
        print_jobs(partitions)
        
        # Copy S3 text files and /tmp JSON files to EFS
        copy_to_efs(datadir, partitions)
        print("Coordinating files copied to EFS directories.")
        
        # # Create and submit job arrays
        # submit = Submit(config, dataset, datadir)
        # job_list = submit.create_jobs(partitions, prefix)
        # try:
        #     job_ids = submit_jobs(job_list)
        #     import json
        #     print(json.dumps(job_ids,indent=2))
        # except botocore.exceptions.ClientError as e:
        #     handle_error(e)
        
        # Delete download text file lists from S3 bucket
        try:
            delete_s3(dataset, prefix, download_lists)
        except botocore.exceptions.ClientError as e:
            handle_error(e)
        
    else:
        print(f"No available licenses. Download lists have been written to the queue: {prefix}-pending-jobs.")
        sys.exit(0)
                    
# if __name__ == "__main__":
#     event_handler(None, None)