import subprocess
import time
from typing import Any, Generic, Optional, Dict, List
from collections import OrderedDict


def parse_sinfo_gpu_info(partition: str = None) -> Dict[Any, Any]:
    query = subprocess.run(['sinfo  -o "%P %G %N" --partition="{}"'.format(partition)],
    shell=True,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE)
    query_parsed = query.stdout.split(b'\n')
    print('query_parsed: ', query_parsed)
parse_sinfo_gpu_info('iris')
