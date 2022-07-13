#!usr/bin/env python3
import subprocess
import time
from typing import Any, Generic, Optional, Dict, List

DAY_CONV = 24 * 60 * 60  # day to sec unit conversion
HOUR_CONV = 60 * 60  # hour to sec unit conversion
MINUTE_CONV = 60  # minute to sec unit conversion


class gpuSpec(object):
    """
    Simple object with slurm job gpu allocation info
    """

    def __init__(self,
            gres: str,
            num_gres: int):
        self.gres = gres
        self.num_gres = num_gres

    def get_num_gres(self,)
        return self.num_gres

    def get_gres(self,)
        return self.gres


while True:
    process = subprocess.run(['squeue -o "%A %C %D %N %p %Q %M %m %b"'],
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
    process_output = process.stdout
    line_parsed_process_output = process_output.split(b'\n')
    fully_preprocessed_output = parse_slurm_squeue_response(
            line_parsed_process_output)
    time.sleep(1.0)


def parse_slurm_queue_response(response:
        List[Sequence[bytes]]
        ) -> Dict[str, Sequence[Any]]:
    """
    Parses and process slurm squeue response.

    Args:
        response: Nested list of bytes.
    Return:
        A processed  dict of <str, Any>.
    """
    # split first line (column names) by column
    keys = response[0].split(b' ')

    # split response into column+row nested list
    response_array = [ppo[i].split(b' ') for i in range(len(response))]
    # remove first stream spec first line and last line artifact
    response_array_clean = response_array[1:-1]
    # tranpose response array
    response_transposed = list(map(list, zip(*response_array_clean)))
    # create dictionary corresponding to columns and rows
    state = dict(zip(keys, response_transposed))
    print(state)


def parse_gpu_info_string(gpu_byte_string:
        List[btyes]
        ) -> gpuSpec:
    """
    Parse GPU info string.

    Args:
        gpu_byte_string: byte string structured 'gres:(optional:type):num_gpus'
    Return:
        gpuSpec object
    """
    separated_gpu_info = gpu_byte_string.split(b':')
    gpu_info = gpuSpec(
                gres=separated_gpu_info[0].decode("utf-8"),
                num_gres=int(separated_gpu_info[-1])
            )
    return gpu_info


def parse_time_info_string(time_byte_string:
        List[bytes]
        ) -> float:
 """
    Parse time info string.

    Args:
        time_byte_string: byte string structured 'd-hour:min:sec'
    Return:
        float corresponding to second resolution.
    """
    separated_time_info = time_byte_string.split(b':')
    if len(separated_time_info) == 3):
        day_hour, minute, seccond=separated_time_info
    else:
        minute, second=separated_time_info
        day_hour=b'0-0'

    day, hour=day_hour.split(b'-')
    day=float(day) * DAY_CONV
    hour=float(hour) * HOUR_CONV
    minute=float(minute) * MINUTE_CONV
    second=float(second)
    time_in_sec=day + hour + minute + second
    return time_in_sec
