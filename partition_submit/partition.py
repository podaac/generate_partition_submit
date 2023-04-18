# -*- coding: utf-8 -*-
"""Parition: Partitions OBPG downloads into chunk-sized jobs.

Parition takes download list creator lists, groups the downloads, and chunks 
them based on the number of available IDL licenses.
"""

# Standard imports
from collections import OrderedDict
import datetime
import json
import math
from pathlib import Path
import random
import time

# Third party imports
import boto3
import botocore
import fsspec
import numpy as np

class Partition:
    """Paritions OBPG downloads into chunk-sized jobs based on IDL licenses.
    
    Attributes
    ----------
    BATCH_SIZE: int
        integer size to batch job array jobs
    combiner: list
        list of files chunked into sublists to be combined
    dataset: str
        string name of dataset
    dlc_lists: list
        list of download list creator output text files
    downloader: list
        list of downloads chunked into sublists
    obpg_files: dictionary
        dictionary of matched and unmatched SST files
    num_lic_avail: integer
        number of IDL licenses avialable
    out_dir: Path
        path to write output data to
    processor: list
        list of files chunked into sublists to be processed
    sst_dict: dictionary
        dictionary with SST file key and SST3/4 or OC file values
    sst_only: list
        List of files that did not have any matched SST3/4 or OC files.
    unique_id: integer
        unique identifier for workflow
    unmatched: list
        list of SST3/4 and OC files that do not have a matching SST file
        
    Methods
    -------
    """
    
    # Constants
    BATCH_SIZE = 10
    
    def __init__(self, dataset, dlc_lists, out_dir, prefix, threshold_quicklook,
                 threshold_refined, logger):
        """
        Attributes
        ----------
        dataset: str
            string name of dataset
        dlc_lists: list
            list of download list creator output text files
            
        Raises
        ------
        botocore.exceptions.ClientError exception.
        """
        
        self.dataset = dataset
        self.datetime_str = datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
        self.dlc_lists = dlc_lists
        self.logger = logger
        self.unique_id = random.randint(1000, 9999)
        try:
            self.num_lic_avail = get_num_lic_avil(dataset, self.unique_id, prefix, self.logger)
        except botocore.exceptions.ClientError as e:
            raise e
        self.obpg_files = {
            "quicklook": [],
            "refined": []
        }
        self.out_dir = Path(out_dir)
        self.prefix = prefix
        self.sst_dict = {}
        self.sst_only = []
        self.sst_process = []
        self.threshold_quicklook = threshold_quicklook
        self.threshold_refined = threshold_refined
        self.unmatched = []
        
    def partition_downloads(self, region, account, prefix):
        """Load all available downloads and partition them based on licenses avialable."""       
        
        # Check the number of available licenses
        if self.num_lic_avail < 2:   # One license per processing type
            try:
                self.update_queue(region, account, prefix)
            except botocore.exceptions.ClientError as e:
                raise e
            return {}, 0
        
        else:
            # Locate downloads
            self.load_downloads(prefix)
            
            # Partition downlaods by SST file timestamp
            if self.sst_dict: 
                self.chunk_downloads_job_array()
                
                # Store unmatched refined SST files for later processing                 
                if len(self.sst_only) > 0: 
                    self.logger.info("Unmatched refined SST files detected.")
                    self.store_sst_only()
                
                # Check if there are any remaining files to submit as AWS Batch jobs   
                jobs_exist = self.check_for_jobs()
                             
                # Write and return JSON files for AWS Batch job submission
                if jobs_exist:
                    job_partitions, downloads_total = self.write_json_files()
                    return job_partitions, downloads_total
                else:
                    return {}, 0
            
            # There are no downloads to process
            else:
                return {}, 0
        
    def update_queue(self, region, account, prefix):
        """Add download lists to queue."""
        
        sqs = boto3.client("sqs")
        
        # Send to queue
        try:
            response = sqs.send_message(
                QueueUrl=f"https://sqs.{region}.amazonaws.com/{account}/{prefix}-pending-jobs-{self.dataset}.fifo",
                MessageBody=json.dumps(self.dlc_lists),
                MessageDeduplicationId=f"{prefix}-{self.dataset}-{self.unique_id}",
                MessageGroupId = f"{prefix}-{self.dataset}"
            )
            self.logger.info(f"Updated pending jobs queue: {self.dlc_lists}.")
        except botocore.exceptions.ClientError as e:
            raise e
        
    def load_downloads(self, prefix):
        """Load downloads and group by SST file."""
        
        # Make a list of all downloads
        downloads = []
        for dlc_list in self.dlc_lists:
            s3_url = f"s3://{prefix}-download-lists/{self.dataset}/{dlc_list}"
            try:
                with fsspec.open(s3_url, mode='r') as fh:
                    downloads.extend(fh.read().splitlines())
                    self.logger.info(f"Downloads retrieved from: {s3_url}.")
            except FileNotFoundError:
                self.logger.error(f"Download list creator txt could not be found: {s3_url}.")
            
        # Load refined SST files
        sst_process = self.load_holding_tank_sst()
        self.sst_process = sst_process    # Track for later
        downloads.extend(sst_process)                
        downloads = list(set(downloads))   # Remove duplicates
        
        # Do not continuing processing if there are no downloads
        if len(downloads) == 0:
            return
        
        # Split into quicklook and refined, Match and group files
        quicklook = [ dl for dl in downloads if "NRT" in dl ]
        self.group_downloads(quicklook, "quicklook")
        
        refined = [ dl for dl in downloads if not "NRT" in dl ]
        self.group_downloads(refined, "refined")
        
    def load_holding_tank_sst(self):
        """Load SST files stored for download that are older than the threshold 
        attributes.
        
        Returns list of downloads.
        """
        
        downloads = []
        s3_client = boto3.client("s3")
        try:
            response = s3_client.list_objects_v2(Bucket=f"{self.prefix}-download-lists", Prefix=f"holding_tank/{self.dataset}")
        except botocore.exceptions.ClientError as e:
            raise e
        
        # List files
        if not "Contents" in response.keys(): 
            self.logger.info("No files were found in the holding tank.")
            return downloads
        if len(response["Contents"]) > 0:
            # quicklook_threshold_time = datetime.datetime.utcnow() - datetime.timedelta(hours=self.threshold_quicklook)
            refined_threshold_time = datetime.datetime.utcnow() - datetime.timedelta(days=self.threshold_refined)
            
            # quicklook_json_files = list(filter(lambda json_file: self.threshold_filter(json_file, "quicklook", quicklook_threshold_time), response["Contents"]))
            refined_json_files = list(filter(lambda json_file: self.threshold_filter(json_file, "refined", refined_threshold_time), response["Contents"]))
            
        # Try load file data
        s3_url = f"s3://{self.prefix}-download-lists"
        # json_files = quicklook_json_files + refined_json_files
        json_files = refined_json_files
        for json_file in json_files:
            try:
                with fsspec.open(f"{s3_url}/{json_file['Key']}", mode='r') as fh:
                    downloads.extend(json.load(fh))
                self.logger.info(f"Loaded SST JSON file: {json_file['Key']}")
                s3_client.delete_object(Bucket=f"{self.prefix}-download-lists", Key=json_file["Key"])
                self.logger.info(f"Deleted SST JSON file: {json_file['Key']}")
            except botocore.exceptions.ClientError as e:    # Delete
                raise e
            except Exception as e:
                self.logger.info(f"Issue with JSON file: {json_file['Key']}.")    # fsspec
                raise e
        
        return downloads
        
    def threshold_filter(self, json_file, ptype, threshold_time):
        """Filter json_files based on threshold value."""
        
        if (ptype in json_file["Key"]):
            json_time = datetime.datetime.strptime(json_file["Key"].split('/')[-1].split('_')[0].split('.')[0], "%Y%m%dT%H%M%S")
            if (json_time <= threshold_time):    # All JSON files older than threshold
                return True
            else:
                return False
        else:
            return False  
        
    def group_downloads(self, downloads, processing_type):
        """Match SST files to appropriate SST3/4 or OC files.
        
        Also saves any SST3/4 or OC files that do not match for download.
        """
        
        # Gather file lists
        sst_list = [ d for d in downloads if "SST.nc" in d or "SST.NRT.nc" in d]
        file_no = 4 if self.dataset == "aqua" or self.dataset == "terra" else 3
        sst34_list = [ d for d in downloads if f"SST{file_no}.nc" in d or f"SST{file_no}.NRT.nc" in d ]
        oc_list = [ d for d in downloads if f"OC.nc" in d or f"OC.NRT.nc" in d ]
        
        # Match
        matched_sst34, matched_oc = self.get_matched(processing_type, sst_list, sst34_list, oc_list, file_no)
        
        # Locate unmatched
        self.unmatched.extend(list(set(sst34_list).difference(matched_sst34)))
        self.unmatched.extend(list(set(oc_list).difference(matched_oc)))
    
    def get_matched(self, processing_type, sst_list, sst34_list, oc_list, file_no):
        """Assigns matched files to sst_dict and returns a tuple of matched 
        SST3/4 and OC files."""
        
        self.sst_dict[processing_type] = {}
        matched_sst = []
        matched_oc = []
        for sst_file in sst_list:
            self.sst_dict[processing_type][sst_file] = {}
            # SST3 and SST4 files
            sst34 = list(filter(lambda sst34_file: self.sst34_filter(sst34_file, sst_file, file_no), sst34_list))
            if len(sst34) > 0:
                self.sst_dict[processing_type][sst_file]["sst34_file"] = sst34[0]
                matched_sst.append(sst34[0])
                
            # OC files
            if self.dataset == "aqua" or self.dataset == "terra":
                oc = list(filter(lambda oc_file: self.oc_filter(oc_file, sst_file), oc_list))
                if len(oc) == 0:
                    oc = list(filter(lambda oc_file: self.oc_time_filter(oc_file, sst_file), oc_list))
                if len(oc) > 0:
                    self.sst_dict[processing_type][sst_file]["oc_file"] = oc[0]
                    matched_oc.append(oc[0])
                    
        return (matched_sst, matched_oc)       
        
    def oc_filter(self, oc_file, sst_file):
        """Filter match for OC file name as compare to SST file."""

        # NRT data product
        if sst_file.find("NRT") > 0:
            oc_url = oc_file.split(' ')[0]
            oc_match_url = f"{sst_file.split(' ')[0][:-11]}.OC.NRT.nc"
            
        # Refined data product
        else: 
            oc_url = oc_file.split(' ')[0]
            oc_match_url = f"{sst_file.split(' ')[0][:-7]}.OC.nc"
        
        if oc_url == oc_match_url:
            return True
        else:
            return False
        
    def oc_time_filter(self, oc_file, sst_file):
        """Filter by OC timestamp in name."""

        oc = oc_file.split(' ')[0].split('/')[-1]
        prefix = sst_file.split(' ')[0].split('/')[-1][:24]
        # Check if there is an oc file available within 60 seconds of sst
        for i in range(60):
            if len(str(i)) == 1:
                if "NRT" in sst_file:
                    updated_oc_file = f"{prefix}0{i}.L2.OC.NRT.nc"
                else:
                    updated_oc_file = f"{prefix}0{i}.L2.OC.nc"
            else:
                if "NRT" in sst_file:
                    updated_oc_file = f"{prefix}{i}.L2.OC.NRT.nc"
                else:
                    updated_oc_file = f"{prefix}{i}.L2.OC.nc"
            
            if updated_oc_file == oc:
                return True
        
        return False
    
    def sst34_filter(self, sst34_file, sst_file, file_no):
        """Filter match for SST4 file name as compared to SST file."""

        # NRT data product
        if sst_file.find("NRT") > 0:
            sst34_url = sst34_file.split(' ')[0]
            sst34_match_url = f"{sst_file.split(' ')[0][:-7]}{file_no}.NRT.nc"
        # Refined data product
        else: 
            sst34_url = sst34_file.split(' ')[0]
            sst34_match_url = f"{sst_file.split(' ')[0][:-3]}{file_no}.nc"
        
        if sst34_url == sst34_match_url:
            return True
        else:
            return False
        
    def chunk_downloads_job_array(self):
        """Sort and partition downloads.
        
        Quicklook is prioritized and listed first.
        Unmatched files are sorted and will be downloaded independent of the
        quicklook or refined batches.
        """
        
        # Sort files and create a large list with quicklook first
        ql_sst_keys = list(OrderedDict(sorted(self.sst_dict["quicklook"].items(), reverse=True)).keys())
        r_sst_keys = list(OrderedDict(sorted(self.sst_dict["refined"].items(), reverse=True)).keys())
        
        # Chunk sst keys based on number of licenses available to form job arrays            
        if len(ql_sst_keys) == 0:
            ql = 0
            r = self.num_lic_avail
            chunked_r_keys = np.array_split(r_sst_keys, r)
            chunked_r_keys = [ x for x in chunked_r_keys if len(x) > 0 ]    # Remove possible empty lists
            for sst in chunked_r_keys:
                self.chunk_and_match(sst, self.sst_dict["refined"], "refined")
        elif len(r_sst_keys) == 0:
            ql = self.num_lic_avail
            chunked_ql_keys = np.array_split(ql_sst_keys, ql)
            chunked_ql_keys = [ x for x in chunked_ql_keys if len(x) > 0 ]    # Remove possible empty lists
            for sst in chunked_ql_keys:
                self.chunk_and_match(sst, self.sst_dict["quicklook"], "quicklook")
            r = 0
        else:
            ql = (self.num_lic_avail) // 2 + (self.num_lic_avail % 2)
            r = self.num_lic_avail // 2
            chunked_ql_keys = np.array_split(ql_sst_keys, ql)
            chunked_ql_keys = [ x for x in chunked_ql_keys if len(x) > 0 ]    # Remove possible empty lists
            for sst in chunked_ql_keys:
                self.chunk_and_match(sst, self.sst_dict["quicklook"], "quicklook")
            chunked_r_keys = np.array_split(r_sst_keys, r)
            chunked_r_keys = [ x for x in chunked_r_keys if len(x) > 0 ]    # Remove possible empty lists
            for sst in chunked_r_keys:
                self.chunk_and_match(sst, self.sst_dict["refined"], "refined")
                  
        # Sort and chunk unmatched by batch size - no license needed
        if len(self.unmatched) != 0:
            self.unmatched.sort(reverse=True)
            batch = math.ceil(len(self.unmatched) / self.BATCH_SIZE)
            self.unmatched = np.array_split(self.unmatched, batch)
            
    def chunk_and_match(self, sst, ptype_dict, obpg_key):
        """Chunk job array into jobs and then match SST files to OC or SST3/4."""
        
        # Chunk the SST files
        batch = math.ceil(len(sst) / self.BATCH_SIZE)
        sst_jobs = np.array_split(sst, batch)
        
        # Match SST files with OC and SST3/4 files
        self.match_chunks(sst_jobs, ptype_dict, obpg_key)
        
    def match_chunks(self, sst_keys, ptype_dict, obpg_key):
        """Organized batches of downloads so that SST files are matched with
        SST3/4 and OC files."""
        
        self.obpg_files[obpg_key].append([])
        for sst_chunk in sst_keys:
               
            l = []
            for sst in sst_chunk:
                
                l.append(sst)
                
                if "sst34_file" in ptype_dict[sst]: 
                    l.append(ptype_dict[sst]["sst34_file"])
                
                if "oc_file" in ptype_dict[sst]: 
                    l.append(ptype_dict[sst]["oc_file"])
                
                # Save sst files with no matching night or day file(s) to place in holding tank    
                if ("sst34_file" not in ptype_dict[sst]) and ("oc_file" not in ptype_dict[sst]):
                    if ("NRT" not in sst) and (sst not in self.sst_process):    # Only applies to refined
                        self.sst_only.append(sst)
                        l.remove(sst)   # Remove from list so not submitted as Batch job
            
            # Add files to submit as jobs or remove placeholder list if none are present
            if len(l) > 0:
                self.obpg_files[obpg_key][-1].append(l)
            else:
                self.obpg_files[obpg_key].pop()
    
    def store_sst_only(self):
        """Store SST files in the download lists S3 bucket under the 
        holding_tank/dataset key.
        
        SST files are stored in JSON files organized by date and hour. This 
        allows the Generate workflow to process SST files on an hourly basis
        which chunks up the processing.
        """
        
        # Sort files
        # quicklook_sst_only = [ sst for sst in self.sst_only if "NRT" in sst ]
        refined_sst_only = [ sst for sst in self.sst_only if "NRT" not in sst ]        
        
        # JSON file name
        date_str = datetime.datetime.utcnow().strftime('%Y%m%dT%H0000')
        # quicklook_json_file = f"{date_str}_quicklook.json"
        refined_json_file = f"{date_str}_refined.json"
        
        # Quicklook
        s3_client = boto3.client("s3")
        # if len(quicklook_sst_only) > 0:
        #     quicklook_exists = self.get_s3_json_file(s3_client, quicklook_json_file)
        #     if quicklook_exists:
        #         self.generate_json(self.out_dir.joinpath(quicklook_json_file), "a", quicklook_sst_only)
        #     else:
        #         self.generate_json(self.out_dir.joinpath(quicklook_json_file), "w", quicklook_sst_only)
        #     self.upload_to_holding_tank(s3_client, self.out_dir.joinpath(quicklook_json_file))     
        
        # Refined
        if len(refined_sst_only) > 0:
            refined_exists = self.get_s3_json_file(s3_client, refined_json_file)
            if refined_exists:
                self.generate_json(self.out_dir.joinpath(refined_json_file), "a", refined_sst_only)
            else:
                self.generate_json(self.out_dir.joinpath(refined_json_file), "w", refined_sst_only)
            self.upload_to_holding_tank(s3_client, self.out_dir.joinpath(refined_json_file))
        
    def get_s3_json_file(self, s3_client, json_file):
        """Check if JSON file exists and download it if it does.
        
        Returns True if exists otherwise False.
        """
        
        try:
            response = s3_client.download_file(f"{self.prefix}-download-lists", f"holding_tank/{self.dataset}/{json_file}", str(self.out_dir.joinpath(json_file)))
            self.logger.info(f"JSON file downloaded: holding_tank/{self.dataset}/{json_file}")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.logger.info(f"JSON file does not exist: holding_tank/{self.dataset}/{json_file}.")
                return False
            else:
                raise e
        return True
    
    def generate_json(self, json_file, mode, sst_only):
        """Either create or modify JSON file to store refined SST files."""
        
        # Append to old file if it exists
        if mode == "a":
            with open(json_file) as jf:
                sst_only.extend(json.load(jf))
                sst_only = [*set(sst_only)]    # Remove any duplicates
                        
        # Write data out
        with open(json_file, mode="w") as jf:
            json.dump(sst_only, jf, indent=2)
            
    def upload_to_holding_tank(self, s3_client, json_file):
        """Upload JSON file to S3 holding tank organized by dataset."""
        
        try:
            response = s3_client.upload_file(str(json_file), f"{self.prefix}-download-lists", f"holding_tank/{self.dataset}/{json_file.name}", ExtraArgs={"ServerSideEncryption": "aws:kms"})
            self.logger.info(f"JSON file uploaded: holding_tank/{self.dataset}/{json_file.name}")
        except botocore.exceptions.ClientError as e:
            raise e
        
    def check_for_jobs(self):
        """Check OBPG files dictionary for jobs."""
        
        jobs_created = False
        for jobs in self.obpg_files.values():
            for job in jobs:
                if len(job) != 0:
                    jobs_created = True
        return jobs_created
        
    def write_json_files(self):
        """Write downloader text files and downloader,combiner and processor
        JSON files."""
        
        # Write txt files and retrieve JSON data
        json_dict = {}
        final_total = 0
        if len(self.obpg_files["quicklook"]) != 0:
            combiner_json, downloader_json, processor_json, total_downloads = self.write_txt_get_json(self.obpg_files["quicklook"], "quicklook")
            combiner_json_lists = self.write_json(combiner_json, f"combiner_file_lists_{self.dataset.upper()}_quicklook")
            downloader_json_lists = self.write_json(downloader_json, f"downloads_file_lists_{self.dataset.upper()}_quicklook")
            processor_json_lists = self.write_json(processor_json, f"processor_timestamp_list_{self.dataset.upper()}_quicklook")
            json_dict["quicklook"] = {
                "combiner": combiner_json_lists,
                "downloader": downloader_json_lists,
                "processor": processor_json_lists,
                "uploader": processor_json_lists,
                "downloader_txt": downloader_json
            }
            final_total += total_downloads
            
        if len(self.obpg_files["refined"]) != 0:
            combiner_json, downloader_json, processor_json, total_downloads = self.write_txt_get_json(self.obpg_files["refined"], "refined")
            combiner_json_lists = self.write_json(combiner_json, f"combiner_file_lists_{self.dataset.upper()}_refined")
            downloader_json_lists = self.write_json(downloader_json, f"downloads_file_lists_{self.dataset.upper()}_refined")
            processor_json_lists = self.write_json(processor_json, f"processor_timestamp_list_{self.dataset.upper()}_refined")
            json_dict["refined"] = {
                "combiner": combiner_json_lists,
                "downloader": downloader_json_lists,
                "processor": processor_json_lists,
                "uploader": processor_json_lists,
                "downloader_txt": downloader_json
            }
            final_total += total_downloads
            
        if len(self.unmatched) != 0:
            downloader_json, total_downloads = self.write_unmatched_json()
            downloader_json_lists = self.write_json(downloader_json, f"downloads_file_lists_{self.dataset.upper()}_unmatched")
            json_dict["unmatched"] = {
                "downloader": downloader_json_lists,
                "downloader_txt": downloader_json
            }
            final_total += total_downloads
        
        return json_dict, final_total 
    
    def write_txt_get_json(self, job_arrays, ptype=None):
        """Write download txt list file and return associated combiner, 
        processor JSON files."""
        
        i = 0
        downloader_json = []
        combiner_json = []
        processor_json = []
        total_downloads = 0
        for job_array in job_arrays:
            txt_files = []
            combiner_jobs = []
            processor_jobs = []
            for jobs in job_array:
                txt_file = f"{self.dataset}_{self.datetime_str}_{i}_{self.unique_id}.txt" if not ptype else f"{self.dataset}_{ptype}_{self.datetime_str}_{i}_{self.unique_id}.txt"
                with open(self.out_dir.joinpath(txt_file), 'w') as fh:
                    for job in jobs:
                        fh.write(f"{job}\n")
                        total_downloads += 1
                txt_files.append(txt_file)
                i += 1
                if ptype == "quicklook":
                    combiner_jobs.append([job.split(' ')[0].split('/')[-1].replace(".NRT", "") for job in jobs])
                else:
                    combiner_jobs.append([job.split(' ')[0].split('/')[-1] for job in jobs])
                p_jobs = list(set([job.split(' ')[0].split('/')[-1].split('.')[1] for job in jobs]))
                p_jobs.sort(reverse=True)
                processor_jobs.append(p_jobs)
            combiner_json.append(combiner_jobs)
            downloader_json.append(txt_files)
            processor_json.append(processor_jobs)
            
        return combiner_json, downloader_json, processor_json, total_downloads
    
    def write_json(self, component_json, filename):
        """Write JSON data for each job array in component JSON."""
        
        i = 0
        filename_list = []
        for json_data in component_json:
            filename_list.append(f"{filename}_{self.datetime_str}_{i}_{self.unique_id}.json")
            with open(self.out_dir.joinpath(f"{filename}_{self.datetime_str}_{i}_{self.unique_id}.json"), 'w') as jf:
                json.dump(json_data, jf, indent=2)
            i += 1
        return filename_list    
    
    def write_unmatched_json(self):
        """Write download file lists and JSON for unmatched file downloads."""
        
        i = 0
        downloader_json = []
        total_downloads = 0
        for job_array in self.unmatched:
            txt_files = []
            txt_file = f"{self.dataset}_unmatched_{self.datetime_str}_{i}_{self.unique_id}.txt"
            with open(self.out_dir.joinpath(txt_file), 'w') as fh:
                for job in job_array:
                    fh.write(f"{job}\n")
                    total_downloads += 1
            txt_files.append(txt_file)
            i += 1
            downloader_json.append(txt_files)
        return downloader_json, total_downloads
        
def get_num_lic_avil(dataset, unique_id, prefix, logger):
    """Get the number of IDL licenses available."""
    
    # Open connection to parameter store
    ssm = boto3.client("ssm")
    
    # Check if another process is trying to retrieve license info and wait until done
    try:
        license_data = open_license(ssm, prefix, dataset)  
        while license_data["retrieving_license"] == "True":
            logger.info("Waiting for license retrieval...")
            time.sleep(3)
            license_data = open_license(ssm, prefix, dataset) 
    except botocore.exceptions.ClientError as e:
        raise e
        
    # Indicate license is going to obtain
    hold_license(ssm, prefix, "True")
        
    # Determine number of licenses available
    num_lic_avail = license_data[dataset]
    license_data[dataset] -= num_lic_avail
    num_floating_avail = 0
    if license_data["floating"] > 0:
        num_floating_avail += 1
        license_data["floating"] -= 1
    # Only update license data if at least 2 license files are available
    if num_lic_avail + num_floating_avail >= 2:
        write_license(ssm, prefix, dataset, license_data, num_lic_avail, num_floating_avail, unique_id)
    
    # Indicate done change license data
    hold_license(ssm, prefix, "False")
    
    return num_lic_avail + num_floating_avail
    
def open_license(ssm, prefix, dataset):
    """Get parameter that indicates whether a license retrieval is already in 
    process."""
    
    license_data = {}
    try:
        license_data["retrieving_license"] = ssm.get_parameter(Name=f"{prefix}-idl-retrieving-license")["Parameter"]["Value"]
        license_data[dataset] = int(ssm.get_parameter(Name=f"{prefix}-idl-{dataset}")["Parameter"]["Value"])
        license_data["floating"] = int(ssm.get_parameter(Name=f"{prefix}-idl-floating")["Parameter"]["Value"])
    except botocore.exceptions.ClientError as e:
        raise e
    return license_data

def hold_license(ssm, prefix, on_hold):
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
        raise e

def write_license(ssm, prefix, dataset, license_data, num_lic_avail, num_floating_avail, unique_id):
    """Write license data to indicate number of licenses in use."""
    
    # Only write out unique license info if enough licenses are available
    if num_lic_avail >= 2:    
        try:
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-{dataset}",
                Type="String",
                Value=str(license_data[dataset]),
                Tier="Standard",
                Overwrite=True
            )
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-floating",
                Type="String",
                Value=str(license_data["floating"]),
                Tier="Standard",
                Overwrite=True
            )
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-{dataset}-{unique_id}-lic",
                Type="String",
                Value=str(num_lic_avail),
                Tier="Standard",
                Overwrite=True
            )
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-{dataset}-{unique_id}-floating",
                Type="String",
                Value=str(num_floating_avail),
                Tier="Standard",
                Overwrite=True
            )
        except botocore.exceptions.ClientError as e:
            raise e
    