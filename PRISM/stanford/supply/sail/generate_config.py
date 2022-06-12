import subprocess
import time
from typing import Any, Generic, Optional, Dict, List
from collections import OrderedDict


def parse_sinfo_gpu_info(partition: str = None) -> Dict[Any, Any]:
    process = subprocess.run(['sinfo  -o "%P %G %N" --partition="{}"'.format(partition)],
    shell=True,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE)
    print(process.stdout)
    return True

parse_sinfo_gpu_info('iris')
