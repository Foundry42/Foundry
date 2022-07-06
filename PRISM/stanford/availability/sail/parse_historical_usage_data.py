import subprocess
import time
from typing import Any, Generic, Optional, Dict, List
from collections import OrderedDict
import json

#######
# Add flags to enable dynamic report generation
#######

DEFAULT_FORMAT = "start,end,elapsed,time,JobID,Jobname,partition,state,MaxRSS,nnodes,ncpus,nodelist,AllocGRES"
def parse_sacct_response(
        start_time: str = None,
        ) -> Dict[Any, Any]:
    """Run subprocess to parse historical data from slurmdb accounting data.
    """
    query = subprocess.run(
            ['sacct --starttime{} --allusers --format={}'.format(start_time, DEFAULT_FORMAT)],
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE
            )
    # split query response by line
    query_parsed = query.stdout.split(b'\n')

    # split per-line query response by column/space and convert into an array
    ##  keys lines is 0
    query_keys = query_parsed[0].split(b' ')
    ## nested list for subsequent rows
    query_response = [
            query_parsed[i].split(b' ') for i in range(len(query_parsed))
            ]
    print('query_response: ', query_response)

    # Filter query response info into a dictionary for easy probing
    sacct_info = {}
    return True

parse_sacct_reponse(start_time="2022-07-04")

