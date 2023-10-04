# -*- coding: utf-8 -*-
"""Submit: Creates and submits job arrays to AWS Batch.

Submit takes the JSON files created from partition and submits them as jobs.
"""

# Third party imports
import boto3
import botocore
import numpy as np

# Local imports
try:
    from job_array import JobArray
except ModuleNotFoundError:
    from partition_submit.job_array import JobArray

class Submit:
    """Submit AWS Batch job arrays based on JSON files created from Partition.
    
    Attributes
    ----------
    
    Methods
    -------
    """
    
    def __init__(self, config_data, dataset, data_dir):
        """
        Arguments
        ----------
        config_file: dict
            Dictionary of AWS Batch job configuration details
        dataset: str
            String name of dataset to submit jobs under
        data_dir: str
            String path location of JSON data files
        """
        
        self.config_data = config_data
        self.dataset = dataset
        self.data_dir = data_dir
        self.job_array_list = []
        self.license_job = None
        self.job_names = []
        self.job_ids = []
                       
    def create_jobs(self, job_partitions, prefix, unique_id):
        """Create AWS Batch job arrays.
        
        Attributes
        ----------
        job_array_dict: dict
            Dictionary of input JSON data files
        prefix: str
            String prefix for AWS infrastructure
        unique_id: integer
            Unique identifier for workflow
        """
        
        # Create component jobs
        job_list = []
        job_count = 0
        for ptype, job_array in job_partitions.items():
            if ptype == "unmatched": continue
            for job in job_array:
                job_list.append([])
                for component, json_data in job.items():
                    if f"{component}_{self.dataset}_{ptype}" not in self.config_data.keys(): continue
                    config_data = self.config_data[f"{component}_{self.dataset}_{ptype}"]
                    job = JobArray(self.dataset, 
                                    ptype, component, 
                                    json_data, config_data, 
                                    self.data_dir,
                                    prefix)
                    if component == "uploader": job.update_command_prefix(prefix)
                    job_list[job_count].append(job)
                job_count += 1

            # License returner job for each processing type
            license_job =  JobArray(self.dataset, "license-returner", "license-returner", None,
                            self.config_data[f"license_returner_{self.dataset}"], 
                            None, prefix)
            license_job.update_command_prefix(prefix)
            license_job.update_command_uid(str(unique_id))
            license_job.update_command_ptype(ptype)
            job_list[-1].append(license_job)
        
        # Add any unmatched downloads to the job list
        if "unmatched" in job_partitions.keys(): job_list.extend(self.append_unmatched_jobs(job_partitions, prefix))
        
        return job_list
    
    def append_unmatched_jobs(self, job_partitions, prefix):
        """Append unmatched downloader jobs to job list."""
        
        unmatched = []
        for json_file in job_partitions["unmatched"]:
            config_data = self.config_data[f"downloader_{self.dataset}_unmatched"]
            unmatched.append([JobArray(self.dataset, 
                                    "unmatched", "downloader", 
                                    json_file, config_data, 
                                    self.data_dir,
                                    prefix)])
        return unmatched
    
    def submit_jobs(self, job_list):
        """Submit jobs to AWS Batch.
        
        Parameters
        ----------
        job_list: list
            list of lists with each sublist a Generate workflow
        """
        
        lr_deps = []
        for jobs in job_list:
            job_ids = []
            job_names = []
            for job in jobs:
                try:
                    if "license-returner" in job.job_name:
                        job_id = submit(job, lr_deps)
                        job_ids.append(job_id)
                        job_names.append(job.job_name)
                    elif len(job_ids) == 0:
                        job_id = submit(job, 0)
                        job_ids.append(job_id)
                        job_names.append(job.job_name)
                    else:
                        job_id = submit(job, job_ids[-1])
                        job_ids.append(job_id)
                        job_names.append(job.job_name)
                    if "processor" in job.job_name:
                        lr_deps.append(job_id)
                except botocore.exceptions.ClientError as error:
                    raise error
            self.job_ids.append(job_ids)
            self.job_names.append(job_names)
            
def submit(job, job_id):
    """Submit job to AWS Batch.
    
    Raises: botocore.exceptions.ClientError
    
    job: JobArray
        JobArray object that contains data needed to submit job to AWS
    index: int
        Integer used to indicate if there are job dependencies
    """
    
    # Boto3 session and client
    client = boto3.client('batch')
    
    # Dependencies
    if job_id == 0:
        job_dependencies = []    # Parallel downloader
    elif isinstance(job_id, list):
        job_dependencies = []
        for jid in job_id:
            job_dependencies.append({"jobId": jid})   # License job
    else:
        job_dependencies = [
            { "jobId": job_id, "type": "N_TO_N" },
            { "type": "SEQUENTIAL" }
        ]
        
    # Job submission
    try:
        response = client.submit_job(
                jobName = job.job_name,
                jobQueue = job.queue,
                jobDefinition = job.job_definition,
                containerOverrides = {
                    "resourceRequirements": [
                        {
                            "type": "MEMORY",
                            "value": job.memory
                        },
                        {
                            "type": "VCPU",
                            "value": job.cpu
                        }
                    ],
                    "command": job.command
                },
                arrayProperties = {} if job.array_size == 1 else { "size": job.array_size },
                dependsOn = job_dependencies,
                shareIdentifier = job.share_identifier,
                schedulingPriorityOverride = job.scheduling_priority
            )
    except botocore.exceptions.ClientError as error:
        raise error
    
    return response["jobId"]   # Job identifier