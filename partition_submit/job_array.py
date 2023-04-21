# -*- coding: utf-8 -*-
"""JobArray: Represents data and methods needed to create an AWS Batch job
array.
"""

# Standard imports
import json
from pathlib import Path

class JobArray():
    """Class that represents an AWS Batch job array.
    
    Attributes
    ----------
    
    Methods
    -------
    """
    
    def __init__(self, dataset, processing_type, component, input_list, config_data, data_dir, prefix):
        """
        Attributes
        ----------
        dataset: str
            String name of dataset to process data for
        processing_type: str
            Indicates either 'quicklook' or 'refined' operations
        component: str
            Name of Generate workflow component
        input_list: str
            Name of input JSON list
        config_data: dict
            Dictionary of job configuration data
        data_dir: str
            String path location of JSON data files
        prefix: str
            String prefix for AWS infrastructure
        """
        
        self.dataset = dataset
        self.processing_type = processing_type
        self.component = component
        self.queue = f"{prefix}-{dataset}"
        self.job_definition = f"{prefix}-{component}"
        self.cpu = config_data["cpu"]
        self.memory = config_data["memory"]
        self.scheduling_priority = 10 if processing_type == "quicklook" else 1
        self.share_identifier = config_data["share_identifier"]
        if input_list is None:
            self.counter = 0
            self.array_size = 1
            self.command = config_data["command"]
            self.job_name = f"{self.queue}-{self.processing_type}"
        else:
            self.counter = input_list.split('.')[0].split('_')[-2]
            self.array_size = get_array_size(Path(data_dir).joinpath(input_list))
            self.command = list(map(lambda x: x.replace("input_list", input_list), config_data["command"]))        
            if self.array_size == 1: 
                self.command = list(map(lambda x: x.replace("-235", "0"), self.command))
            self.job_name = f"{self.queue}-{self.processing_type}-{self.component}-{self.counter}"
        
    def update_command_prefix(self, prefix):
        """Update uploader command with method parameters."""
        
        self.command = list(map(lambda x: x.replace("prefix", prefix), self.command))
        
    def update_command_uid(self, unique_id):
        """Update uploader command with method parameters."""
        
        self.command = list(map(lambda x: x.replace("unique_id", unique_id), self.command))
        
    def update_command_ptype(self, ptype):
        """Update uploader command with method parameters."""
        
        self.command = list(map(lambda x: x.replace("ptype", ptype), self.command))
        
def get_array_size(input_list):
    """Determines array size from contents of input list file.
    
    Parameter
    ---------
    input_list: Path
        Path to input JSON list file
    """
    
    with open(input_list) as jf:
        data = json.load(jf)
    return len(data)