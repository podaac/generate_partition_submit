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
import json
import logging
import pathlib
import shutil
import sys
import time

# Third-party imports
import boto3
import botocore
import fsspec

# Local imports
from notify import notify
from partition import Partition
from submit import Submit

# Constants
EFS_DIR = pathlib.Path("/mnt/data")
EFS_DIRS = {
    "downloader": "/mnt/data/downloader/lists",
    "combiner": "/mnt/data/combiner/downloads",
    "processor": "/mnt/data/processor/input"
}

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
    processor.joinpath("logs", "error_logs").mkdir(parents=True, exist_ok=True)
    processor.joinpath("logs", "processing_logs").mkdir(parents=True, exist_ok=True)
    processor.joinpath("logs", "seatmp_manager").mkdir(parents=True, exist_ok=True)
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
        for i in range(len(partitions[ptype]["downloader"])):
            shutil.copyfile(f"{datadir}/{partitions[ptype]['downloader'][i]}", f"{EFS_DIRS['downloader']}/{partitions[ptype]['downloader'][i]}")
            if ptype == "unmatched": continue
            shutil.copyfile(f"{datadir}/{partitions[ptype]['combiner'][i]}", f"{EFS_DIRS['combiner']}/{partitions[ptype]['combiner'][i]}")
            shutil.copyfile(f"{datadir}/{partitions[ptype]['processor'][i]}", f"{EFS_DIRS['processor']}/{partitions[ptype]['processor'][i]}")  
            
def delete_s3(dataset, prefix, downloads_list, logger):
    """Delete DLC-created download lists from S3 bucket."""
    
    s3 = boto3.client("s3")
    for txt_file in downloads_list:
        try:
            response = s3.delete_object(Bucket=f"{prefix}-download-lists",
                                        Key=f"{dataset}/{txt_file}")
            logger.info(f"S3 file deleted: {dataset}/{txt_file}")      
        except botocore.exceptions.ClientError as e:
            raise e  
        
def get_logger():
    """Return a formatted logger object."""
    
    # Remove AWS Lambda logger
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    
    # Create a Logger object and set log level
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Create a handler to console and set level
    console_handler = logging.StreamHandler()

    # Create a formatter and add it to the handler
    console_format = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s : %(message)s")
    console_handler.setFormatter(console_format)

    # Add handlers to logger
    logger.addHandler(console_handler)

    # Return logger
    return logger  

def handle_error(error, unique_id, prefix, dataset, logger):
    """Print out error message, notify users, return licenses, and exit."""
    
    logger.error(f"Error encountered: {type(error)}.")
    logger.error(error)
    try:
        return_licenses(unique_id, prefix, dataset, logger)
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error trying to restore reserved IDL licenses.")
        logger.error(e)
    notify(logger, "ERROR", error, type(error))
    logger.error("System exiting.")
    sys.exit(1)
    
def return_licenses(unique_id, prefix, dataset, logger):
    """Return licenses that were reserved for current workflow."""
    
    ssm = boto3.client("ssm", region_name="us-west-2")
    try:
        # Get number of licenses that were used in the workflow
        dataset_lic = ssm.get_parameter(Name=f"{prefix}-idl-{dataset}-{unique_id}-lic")["Parameter"]["Value"]
        floating_lic = ssm.get_parameter(Name=f"{prefix}-idl-{dataset}-{unique_id}-floating")["Parameter"]["Value"]
        
        # Wait until no other process is updating license info
        retrieving_lic =  ssm.get_parameter(Name=f"{prefix}-idl-retrieving-license")["Parameter"]["Value"]
        while retrieving_lic == "True":
            logger.info("Watiing for license retrieval...")
            time.sleep(3)
            retrieving_lic =  ssm.get_parameter(Name=f"{prefix}-idl-retrieving-license")["Parameter"]["Value"]
        
        # Place hold on licenses so they are not changed
        hold_license(ssm, prefix, "True", logger)  
        
        # Return licenses to appropriate parameters
        write_licenses(ssm, dataset_lic, floating_lic, prefix, dataset, logger)
        
        # Release hold as done updating
        hold_license(ssm, prefix, "False", logger)
        
        # Delete unique parameters
        response = ssm.delete_parameters(
            Names=[f"{prefix}-idl-{dataset}-{unique_id}-lic",
                    f"{prefix}-idl-{dataset}-{unique_id}-floating"]
        )
        logger.info(f"Deleted parameter: {prefix}-idl-{dataset}-{unique_id}-lic")
        logger.info(f"Deleted parameter: {prefix}-idl-{dataset}-{unique_id}-floating")
        
    except botocore.exceptions.ClientError as e:
        raise e

def hold_license(ssm, prefix, on_hold, logger):
        """Put parameter license number ot use indicating retrieval in process."""
        
        try:
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-retrieving-license",
                Type="String",
                Value=on_hold,
                Tier="Standard",
                Overwrite=True
            )
        except botocore.exceptions.ClientError as e:
            hold_action = "place" if on_hold == "True" else "remove"
            logger.error(f"Could not {hold_action} a hold on licenses...")
            raise e
        
def write_licenses(ssm, dataset_lic, floating_lic, prefix, dataset, logger):
    """Write license data to indicate number of licenses ready to be used."""
    
    try:
        current = ssm.get_parameter(Name=f"{prefix}-idl-{dataset}")["Parameter"]["Value"]
        total = int(dataset_lic) + int(current)
        response = ssm.put_parameter(
            Name=f"{prefix}-idl-{dataset}",
            Type="String",
            Value=str(total),
            Tier="Standard",
            Overwrite=True
        )
        current_floating = ssm.get_parameter(Name=f"{prefix}-idl-floating")["Parameter"]["Value"]
        floating_total = int(floating_lic) + int(current_floating)
        response = ssm.put_parameter(
            Name=f"{prefix}-idl-floating",
            Type="String",
            Value=str(floating_total),
            Tier="Standard",
            Overwrite=True
        )
        logger.info(f"Wrote {dataset_lic} license(s) to {dataset}.")
        logger.info(f"Wrote {floating_lic} license(s)to floating.")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Could not return {dataset} and floating licenses...")
        raise e
    
def print_jobs(partitions, logger):
    """Print the number of jobs per component and processing type."""
    
    if "quicklook" in partitions.keys():
        logger.info(f"Number of quicklook downloader jobs: {len(partitions['quicklook']['downloader'])}")
        logger.info(f"Number of quicklook combiner jobs: {len(partitions['quicklook']['combiner'])}")
        logger.info(f"Number of quicklook processor jobs: {len(partitions['quicklook']['processor'])}")
        logger.info(f"Number of quicklook uploader jobs: {len(partitions['quicklook']['uploader'])}")
        
    if "refined" in partitions.keys():
        logger.info(f"Number of refined downloader jobs: {len(partitions['refined']['downloader'])}")
        logger.info(f"Number of refined combiner jobs: {len(partitions['refined']['combiner'])}")
        logger.info(f"Number of refined processor jobs: {len(partitions['refined']['processor'])}")
        logger.info(f"Number of refined uploader jobs: {len(partitions['refined']['uploader'])}")
    
    if "unmatched" in partitions.keys():
        logger.info(f"Number of unmatched downloader jobs: {len(partitions['unmatched']['downloader'])}")

def read_config(prefix):
    """Read in JSON config file for AWS Batch job submission."""
    
    s3_url = f"s3://{prefix}-download-lists/config/job_config.json"
    with fsspec.open(s3_url, mode='r') as fh:
        job_config = json.load(fh)
    return job_config

def event_handler(event, context):
    """AWS Lambda event handler that kicks off partition of data and submits
    AWS Batch jobs."""
    
    # Arguments
    account = event["Records"][0]["eventSourceARN"].split(':')[4]
    region = event["Records"][0]["awsRegion"]
    body = event["Records"][0]["body"].replace("'", '"')
    body = json.loads(body)
    dataset = body["dataset"]
    datadir = "/tmp"
    prefix = body["prefix"]
    config = read_config(prefix)
    download_lists = body["txt_list"]
    
    # Logger
    logger = get_logger()
    
    # Partition
    try:
        partition = Partition(dataset, download_lists, datadir, prefix, logger)
        partitions, total_downloads = partition.partition_downloads(region, account, prefix)
        logger.info(f"Unique idenitifier: {partition.unique_id}")
        logger.info(f"Number of licenses available: {partition.num_lic_avail}.")
        logger.info(f"Total number of downloads: {total_downloads}")
    except botocore.exceptions.ClientError as e:
        handle_error(e, partition.unique_id, prefix, dataset, logger)
    except FileNotFoundError as e:
        handle_error(e, partition.unique_id, prefix, dataset, logger)
    
    # If there are downloads and available licenses, then submit jobs
    if partitions:
        print_jobs(partitions, logger)
        
        # Copy S3 text files and /tmp JSON files to EFS
        copy_to_efs(datadir, partitions)
        logger.info("Coordinating files copied to EFS directories.")
        
        # Create and submit job arrays
        submit = Submit(config, dataset, datadir)
        job_list = submit.create_jobs(partitions, prefix, partition.unique_id)
        try:
            job_ids = submit.submit_jobs(job_list, logger)
            for job_id in job_ids:
                logger.info(f"Job executing: {job_id}")
            # print(json.dumps(job_ids,indent=2))
        except botocore.exceptions.ClientError as e:
            handle_error(e, partition.unique_id, prefix, dataset, logger)
        
        # Delete download text file lists from S3 bucket
        try:
            delete_s3(dataset, prefix, download_lists, logger)
        except botocore.exceptions.ClientError as e:
            handle_error(e, partition.unique_id, prefix, dataset, logger)
        
    else:
        logger.info(f"No available licenses. Download lists have been written to the queue: {prefix}-pending-jobs.")