# Standard imports
import json
import pathlib
import unittest
from unittest.mock import patch

# Third-party imports
import numpy as np

# Local imports
from partition_submit.partition import Partition
from partition_submit.submit import Submit

class TestPartitionSubmit(unittest.TestCase):
    """Tests partition and job organization methods of Partition and Submit."""
    
    TEST_DIR = pathlib.Path(__file__).parent / "test_data"
    
    SST_DICT = TEST_DIR.joinpath("partition_sst_dict.json")
    UNMATCHED = TEST_DIR.joinpath("partition_unmatched.json")
    PARTITION_RESULTS = TEST_DIR.joinpath("partition_results.json")
    UNMATCHED_RESULTS = TEST_DIR.joinpath("partition_unmatched_results.json")
    
    SUBMIT_PARTITIONS = TEST_DIR.joinpath("submit_partitions.json")
    SUBMIT_CONFIG = TEST_DIR.joinpath("job_config.json")
    SUBMIT_RESULTS = TEST_DIR.joinpath("submit_results.json")
    
    @patch("partition_submit.partition.get_num_lic_avil", autospec=True)
    def test_partition_chunk_downloads_job_array(self, num_lic_mock):
        """Test chunk_downloads_job_array method of Partition class."""
        
        # Set return value for mock
        num_lic_mock.return_value = 5
        
        # Initialize Partition class
        partition = Partition("aqua", None, "None", None)
        
        # Load test data
        with open(self.SST_DICT) as jf:
            sst_dict = json.load(jf)
            
        with open(self.UNMATCHED) as jf:
            unmatched = json.load(jf)
        
        # Execute method
        partition.BATCH_SIZE = 5
        partition.sst_dict = sst_dict
        partition.unmatched = unmatched
        partition.chunk_downloads_job_array()
        
        # Assert results
        with open(self.PARTITION_RESULTS) as jf:
            sst_results = json.load(jf)
        
        with open(self.UNMATCHED_RESULTS) as jf:
            unmatched_results = json.load(jf)
            
        actual = np.array(partition.obpg_files["quicklook"], dtype="object")
        expected = np.array(sst_results["quicklook"], dtype="object")
        np.testing.assert_array_equal(actual, expected)  
        
        actual = np.array(partition.obpg_files["refined"], dtype="object")
        expected = np.array(sst_results["refined"], dtype="object")
        np.testing.assert_array_equal(actual, expected)  
        
        for i in range(len(partition.unmatched)):
            np.testing.assert_array_equal(partition.unmatched[i].tolist(), unmatched_results[i])
            
    def test_submit_create_jobs(self):
        """Test create_jobs method of Submit class."""
        
        # Initialize Submit class
        submit = Submit(self.SUBMIT_CONFIG, "aqua", self.TEST_DIR.joinpath("out"))
        
        # Load test data
        with open(self.SUBMIT_PARTITIONS) as jf:
            partitions = json.load(jf)
        
        # Execute method
        job_list = submit.create_jobs(partitions, "podaac-sndbx-generate")
        
        # Assert expected results
        actual = []
        for jobs in job_list:
            actual.append([job.job_name for job in jobs])
        with open(self.SUBMIT_RESULTS) as jf:
            expected = json.load(jf)
        actual = np.array(actual, dtype="object")
        expected = np.array(expected, dtype="object")
        np.testing.assert_array_equal(actual, expected)