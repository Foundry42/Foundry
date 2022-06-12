import subprocess
import time
from typing import Any, Generic, Optional, Dict, List
from collections import OrderedDict


def parse_parition_gpu_info(partition: str = None) -> Dict[Any, Any]:
    # Run initial subprocess to query for partition information
    query = subprocess.run(
    ['sinfo  -o "%P %G %N" --partition="{}"'.format(partition)],
    shell=True,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE)

    # Split query response by line
    query_parsed = query.stdout.split(b'\n')

    # Split per-line query response by column/space into array
    # keys line 0
    query_keys = query_parsed[0].split(b' ')
    # nested list for subsequent rows
    query_response = [
    query_parsed[i].split(b' ') for i in range(len(query_parsed))
    ]
    print('query_response: ', query_response)





parse_parition_gpu_info('iris')
