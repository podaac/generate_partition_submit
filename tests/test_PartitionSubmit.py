# Standard imports
import json
import pathlib
import unittest
from unittest.mock import patch

# Third-party imports
import numpy as np

# Local imports
from partition_submit.partition import Partition

# test_dir = pathlib.Path(__file__).parent / "test_data"
# dlc_lists = ["modis_aqua_filelist.txt.daily_2023_024_date_2023_01_24", "modis_aqua_filelist.txt.daily_2023_025_date_2023_01_25", "modis_aqua_filelist.txt.daily_2022_156_date_2022_06_05"]
# partition = Partition("aqua", dlc_lists, test_dir.parent, "podaac-sndbx-generate")
# partition.load_downloads(None, None, None, "podaac-sndbx-generate")
# print(partition.unmatched)
# print("---")
# # print(partition.sst_dict)
# for ptype, sst_files in partition.sst_dict.items():
#     print(ptype)
#     for k,v in sst_files.items():
#         print("\t", k)
#         print("\t\t", v)


class TestPartitionSubmit(unittest.TestCase):
    """Tests partition and job organization methods of Partition and Submit."""
    
    TEST_DIR = pathlib.Path(__file__).parent / "test_data"
    SST_DICT = TEST_DIR.joinpath("partition_sst_dict.json")
    PARTITION_RESULTS = TEST_DIR.joinpath("partition_results.json")
    
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
        
        # Execute method
        partition.BATCH_SIZE = 5
        partition.sst_dict = sst_dict
        partition.chunk_downloads_job_array()
        
        # Assert results
        with open(self.PARTITION_RESULTS) as jf:
            sst_results = json.load(jf)
        
        actual = np.array(partition.obpg_files["quicklook"], dtype="object")
        expected = np.array(sst_results["quicklook"], dtype="object")
        np.testing.assert_array_equal(actual, expected)  
        
        actual = np.array(partition.obpg_files["refined"], dtype="object")
        expected = np.array(sst_results["refined"], dtype="object")
        np.testing.assert_array_equal(actual, expected)  