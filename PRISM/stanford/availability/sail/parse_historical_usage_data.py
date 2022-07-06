import subprocess
import time
from typing import Any, Generic, Optional, Dict, List
from collections import OrderedDict
import json
from datetime import datetime

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
    return True


parse_sacct_response(start_time="2022-07-04")
date_interpreter(b'2019-01-07T17:15:32')

