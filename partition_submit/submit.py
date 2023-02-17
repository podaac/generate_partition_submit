# -*- coding: utf-8 -*-
"""Submit: Creates and submits job arrays to AWS Batch.

Submit takes the JSON files created from partition and submits them as jobs.
"""

# Standard imports
import json

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
        Attributes
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
                       
    def create_jobs(self, job_array_dict, prefix, unique_id, last_job_index):
        """Create AWS Batch job arrays.
        
        Attributes
        ----------
        job_array_dict: dict
            Dictionary of input JSON data files
        prefix: str
            String prefix for AWS infrastructure
        unique_id: integer
            Unique identifier for workflow
        last_job_index: integer
            Last index of AWS Batch job
        """
        
        # Create jobs
        job_dict = {}
        num_ql = 0
        num_r = 0
        for ptype, component_dict in job_array_dict.items():
            if ptype == "unmatched": continue    # Handle unmatched downloads later
            job_dict[ptype] = {}
            for component, json_list in component_dict.items():
                if component == "downloader_txt": continue
                job_dict[ptype][component] = []
                config_data = self.config_data[f"{component}_{self.dataset}_{ptype}"]
                for json_file in json_list:
                    job = JobArray(self.dataset, 
                                   ptype, component, 
                                   json_file, config_data, 
                                   self.data_dir,
                                   prefix)
                    if component == "uploader": job.update_uploader_command(prefix, unique_id, last_job_index)
                    job_dict[ptype][component].append(job)
                    if ptype == "quicklook": num_ql +=1
                    if ptype == "refined": num_r += 1           
        # Organize jobs
        job_list = organize_jobs(job_dict, num_ql, num_r)
        
        # Add any unmatched downloads to the job list
        if "unmatched" in job_array_dict.keys(): job_list.append(self.append_unmatched_jobs(job_array_dict, prefix))

        return job_list
    
    def append_unmatched_jobs(self, job_array_dict, prefix):
        """Append unmatched downloader jobs to job list."""
        
        unmatched = []
        for component, json_files in job_array_dict["unmatched"].items():
            for json_file in json_files:
                if component == "downloader_txt": continue
                config_data = self.config_data[f"{component}_{self.dataset}_unmatched"]
                unmatched.append(JobArray(self.dataset, 
                                        "unmatched", component, 
                                        json_file, config_data, 
                                        self.data_dir,
                                        prefix))
        return unmatched
                    
def organize_jobs(job_dict, num_ql, num_r):
    """Organize JobArray jobs in job list by component and counter.
    
    Returns a list of lists with each sublist representative of a Generate
    workflow. The lists can be executed in parallel.
    
    TODO Error handling when 3 components are not found?
    
    Parameter
    --------
    job_dict: dict
        Dictionary of JobArray objects
    """
    
    quicklook = np.empty(shape=((num_ql//4), 4), dtype=object)
    refined = np.empty(shape=((num_r//4), 4), dtype=object)
    for ptype, components in job_dict.items():
            for component, job_list in components.items():
                for job in job_list:
                    j = int(job.counter)
                    if ptype == "quicklook":
                        if component == "downloader": quicklook[j,0] = job 
                        if component == "combiner": quicklook[j,1] = job 
                        if component == "processor": quicklook[j,2] = job 
                        if component == "uploader": quicklook[j,3] = job
                    if ptype == "refined":
                        if component == "downloader": refined[j,0] = job 
                        if component == "combiner": refined[j,1] = job 
                        if component == "processor": refined[j,2] = job 
                        if component == "uploader": refined[j,3] = job
    
    return quicklook.tolist() + refined.tolist()

def submit_jobs(job_list):
    """Submit jobs to AWS Batch.
    
    Parameters
    ----------
    job_list: list
        list of lists with each sublist a Generate workflow
    """
    
    dataset_job_ids = []
    for jobs in job_list:
        job_ids = []
        for job in jobs:
            try:
                if len(job_ids) == 0:
                    job_ids.append(submit(job, 0))
                else:
                    job_ids.append(submit(job, job_ids[-1]))
            except botocore.exceptions.ClientError as error:
                raise error
        dataset_job_ids.append(job_ids)
    return dataset_job_ids
            
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

    print(f"Job submitted: {response['jobName']}")
    return response["jobId"]   # Job identifier