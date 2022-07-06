import subprocess
import time
from typing import Any, Generic, Optional, Dict, List
from collections import OrderedDict, namedtuple
import json
from datetime import datetime

#######
# Add flags to enable dynamic report generation
#######

DEFAULT_FORMAT = "start,end,elapsed,time,JobID,Jobname,partition,state,MaxRSS,nnodes,ncpus,nodelist,AllocGRES"
DEFAULT_FORMAT_KEY_DICT = OrderedDict(zip(DEFAULT_FORMAT.split(','),list(range(len(DEFAULT_FORMAT)))))
class Dict2Class(object):
    def __init__(self, init_dict):
        self.init_dict = init_dict
        for key in init_dict:
            setattr(self,key,init_dict[key])
    def __str__(self):
        return str(print(self.init_dict))

DFK = Dict2Class(DEFAULT_FORMAT_KEY_DICT)
def parse_sacct_response(
        start_time: str,
        key: Any,
        ) -> Dict[Any, Any]:
    """Run subprocess to parse historical data from slurmdb accounting data.
    """
    query = subprocess.run(
            ['sacct --starttime {} --allusers --format={}'.format(start_time, DEFAULT_FORMAT)],
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE
            )
    # split query response by line
    query_parsed = query.stdout.split(b'\n')
    # split per-line query response by column/space and convert into an array
    ##  keys lines is 0
    query_keys = query_parsed[0].split()
    ## nested list for subsequent rows
    query_response = [
            query_parsed[i].split() for i in range(len(query_parsed))
            ]
    # convert date string into a number format 
    # Filter query response info into a dictionary for easy probing
    sacct_info = {}
    # Note - index from 2 to remove key line and delimiter line
    for job in query_response[2:-1]:
        job_id = job[key.JobID] 
        job_prime = job
        job_prime[key.start] = date_interpreter(job[key.start])
        job_prime[key.end] = date_interpreter(job[key.end])
        sacct_info[job_id] = job_prime
    print(sacct_info)
    return sacct_info

def date_interpreter(
        date: bytes,
        ) -> float:
    """Converts date byte string to int so we can do inclusion comparison.
    
    Args:
        date: format b'2019-01-07T17:15:32'
    Returns:
        Floating point number. 
    """
    if date == b'Unknown':
        return float('inf')
    # Decode byte string into a standard string.
    ## explicit version - brute force, not using datetime api
    ### convert byte string to string
    date_time = date.decode("utf-8")
    ### split date string by date and time
    date_time = date_time.split('T')
    date, time = date_time
    year, month, day = [int(primitive) for primitive in date.split('-')]
    hour, minute, seconds = [int(primitive) for primitive in time.split(':')]
    datetime_object = datetime(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            second=seconds
            )
    return datetime_object

def map_sacct_info(
        start_time: str,
        sacct_info: Dict[Any, Any]
        resolution_in_minutes: int = 30
        ) -> Dict[Any, Any]:
    """Map saact info query respone to a time-indexed object.
    """
    # Create keys for time-indexed object.

parse_sacct_response(start_time="2022-07-04", key=DFK)
date_interpreter(b'2019-01-07T17:15:32')

