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
DEFAULT_FORMAT_KEY = namedtuple('DEFAULT_FORMAT_KEY', DEFAULT_FORMAT.replace(',', ' '))
DFK = DEFAULT_FORMAT_KEY(
        start=0,
        end=1,
        elapsed=2,
        time=3,
        JobID=4,
        Jobname=5,
        partition=6,
        state=7,
        MaxRSS=8,
        nnodes=9,
        ncpus=10,
        nodelist=11,
        AllocGRES=12
        )

class SacctStruct:
    def __init__(self, **data):
        self.__dict__.update(data)

print('DFK: ', DFK)
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
    print('query_response: ', query_response[0:3])
    # convert date string into a number format 
    # Filter query response info into a dictionary for easy probing
    sacct_info = {}
    for job in query_response[1:-1]:
        job_id = job[key.job_id]
        job_prime = job
        job_prime[key.start] = data_interpreter(job[key.start])
        job_prime[key.end] = data_interpreter(job[key.end])
        sacct_info[job_id] = job
    return True

def date_interpreter(
        date: bytes,
        ) -> float:
    """Converts date byte string to int so we can do inclusion comparison.
    
    Args:
        date: format b'2019-01-07T17:15:32'
    Returns:
        Floating point number. 
    """
    # Decode byte string into a standard string.
    ## explicit version - brute force, not using datetime api
    ### convert byte string to string
    date_time = date.decode("utf-8")
    ### split date string by date and time
    date_time = date_time.split('T')
    date, time = date_time
    year, month, day = [int(primitive) for primitive in date.split('-')]
    print(year, month, day)
    hour, minute, seconds = [int(primitive) for primitive in time.split(':')]
    print(hour, minute, seconds)
    datetime_object = datetime(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            second=seconds
            )
    print(datetime_object)
    return datetime_object



parse_sacct_response(start_time="2022-07-04")
date_interpreter(b'2019-01-07T17:15:32')

