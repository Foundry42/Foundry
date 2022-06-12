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
    print('query_response: ', query_response

    # Per line in query_response, accumulate info into dict for config
    # cleaning query response to remove keys and end token
    iterable_query = query_resonse[1:-1]
    for line in iterable_query:
        #TODO(jq): get rid of hardcoded keys
        partition_name = line[0]
        gres = line[1]
        nodelist = line[2]

        # parse_gres
        parse_gres(gres)

def parse_gres(gres: byte_string = None) -> List[byte_string]:
    print(gres)




parse_parition_gpu_info('iris')
