#!usr/bin/env python3
import subprocess
import time
from typing import Any, Generic, Optional, Dict, List, Sequence
from collections import OrderedDict
# from absl import logging

DAY_CONV = 24 * 60 * 60  # day to sec unit conversion
HOUR_CONV = 60 * 60  # hour to sec unit conversion
MINUTE_CONV = 60  # minute to sec unit conversion
KEY_INDEX = {
        'job-id': 0,
        'partition': 1,
        'time': 2,
        'gpus': 3

        }
SINFO_KEY_INDEX = {
        'partition': 0,
        'gpus':2,
        'node_list': -1
        }

class gpuSpec(object):
    """
    Simple object with slurm job gpu allocation info
    """
    def __init__(self,
            gres: str,
            gres_type: str,
            num_gres: int):
        self.gres = gres
        self.gres_type = gres_type
        self.num_gres = num_gres

    def get_num_gres(self,):
        return self.num_gres

    def get_gres(self,):
        return self.gres

# Helper Functions
def dict_safe_add_list(
        dictionary: Dict[Any, Any],
        key: Any,
        value: List[Any]
        ) -> Dict[Any, List[Any]]:
    """
    Modify a dictionary to add item to k: [v_1, ...., v_n].

    Args:
        Key: dict key.
        Value: dict value.
Returns:
        Modified dictionary.
    """
    if key not in dictionary:
        dictionary[key] = [value]
    else:
        dictionary[key] += [value]
"master_utilization_monitoring_04032022.py" 459L, 15775C                                               1,1           Top
#!usr/bin/env python3
import subprocess
import time
from typing import Any, Generic, Optional, Dict, List, Sequence
from collections import OrderedDict
# from absl import logging

DAY_CONV = 24 * 60 * 60  # day to sec unit conversion
HOUR_CONV = 60 * 60  # hour to sec unit conversion
MINUTE_CONV = 60  # minute to sec unit conversion
KEY_INDEX = {
        'job-id': 0,
        'partition': 1,
        'time': 2,
        'gpus': 3

        }
SINFO_KEY_INDEX = {
        'partition': 0,
        'gpus':2,
        'node_list': -1
        }

class gpuSpec(object):
    """
    Simple object with slurm job gpu allocation info
    """
    def __init__(self,
            gres: str,
            gres_type: str,
            num_gres: int):
        self.gres = gres
        self.gres_type = gres_type
        self.num_gres = num_gres

    def get_num_gres(self,):
        return self.num_gres

    def get_gres(self,):
        return self.gres

# Helper Functions
def dict_safe_add_list(
        dictionary: Dict[Any, Any],
        key: Any,
        value: List[Any]
        ) -> Dict[Any, List[Any]]:
    """
    Modify a dictionary to add item to k: [v_1, ...., v_n].

    Args:
        Key: dict key.
        Value: dict value.
Returns:
        Modified dictionary.
    """
    if key not in dictionary:
        dictionary[key] = [value]
    else:
        dictionary[key] += [value]
    return dictionary

def dict_safe_sum_int(
        dictionary: Dict[Any, Any],
        key: Any,
        value: int
       ) -> Dict[Any, int]:
    """
    Insert element into dictionary safely or sum if element already exists

    Args:
        Key: dict key.
        Value: dict value.
    Returns:
        Modified dictionary.
    """
    if key not in dictionary:
        dictionary[key] = value
    else:
        dictionary[key] += value
    return dictionary


def check_string_is_num(string: str) -> bool:
    """
    Simple function to check that byte string corresponds to number.
    Assumes continuity in byte-string representation of numbers.
    """
    if string.isnumeric():
        return True
    else:
        return False


def interpret_node_list(
        interim_node_rep: List[str],
        ) -> int:
    """
    Parse list of nodes and return the number of node subclusters.

    Args:
        interim_node_rep:  list of nodes where input string was parsed by ','
    Returns:
        number of node subclusters.
    """
    num_nodes = 0
    for byte_substring in interim_node_rep:
        dash_split = byte_substring.split('-')
        if len(dash_split) == 1:
            num_nodes += 1
        elif len(dash_split) == 2: # split by dash
            # if numbers, convert, subtract and return
            a = dash_split[0][-1] # grab last character of first
            x = dash_split[0][-2:] # grab last 2 in case 10-99
            m = dash_split[0][-3:]
            b = dash_split[-1][0] # grab first character of last
            y = dash_split[-1][0:2].split(']')[0] # grab first two in case 10-99
            n = dash_split[-1][0:3].split(']')[0]
            # if all  char values, return
            if check_string_is_num(a) and check_string_is_num(b):
                if check_string_is_num(m):
                    first_num = int(m)
                elif check_string_is_num(x):
                    first_num = int(x)
                else:
                    first_num = int(a)
                if check_string_is_num(n):
                    second_num = int(n)
                elif check_string_is_num(y):
                    second_num = int(y)
                else:
                    second_num = int(b)
                num_nodes += (second_num - first_num + 1)
            else:
                num_nodes += 1
        else:
            num_nodes += 1
    return num_nodes



    return num_nodes
def parse_slurm_squeue_response(response:
        List[Sequence[Any]]
        ) -> Dict[str, Sequence[bytes]]:
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
    response_array = [response[i].split(b' ') for i in range(len(response))]
    # remove first stream spec first line and last line artifact
    response_array_clean = response_array[1:-1]
    # tranpose response array
    response_transposed = list(map(list, zip(*response_array_clean)))
    # create dictionary corresponding to columns and rows
    state = OrderedDict(zip(keys, response_transposed))
    time_in_sec = [parse_time_info_string(state[b'TIME'][i]) for i in range(
        len(state[b'TIME']))]
    gpu_info = [parse_gpu_info_string(
        state[b'GRES'][i],
        job_time=time_in_sec[i]
        ) for i in range(len(state[b'GRES']))]
    # Use original array to group GPU requests by partition
    lab_partition = {}
    lab_partitiont = {}
    for line in response_array_clean:
        full_partition_name = line[KEY_INDEX['partition']]
        partition_name = full_partition_name.split(b'-')[0]
        partition_name = partition_name.split(b'*')[0]
        gpu_request = line[KEY_INDEX['gpus']]
        time_byte_string = line[KEY_INDEX['time']]
        job_time = parse_time_info_string(time_byte_string)
        if partition_name not in lab_partition.keys():
            lab_partition[partition_name] = [gpu_request]
            lab_partitiont[partition_name] = [job_time]
        else:
            lab_partition[partition_name] += [gpu_request]
            lab_partitiont[partition_name] += [job_time]
    # post process
    for partition, gpu_details in lab_partition.items():
        job_time_details = lab_partitiont[partition]
        lab_partition[partition] = [
                parse_gpu_info_string(
                    gpu_details[i], job_time_details[i])
                for i in range(len(gpu_details))
                ]
        lab_partition[partition] = aggregate_gpu_info(
                lab_partition[partition],
                print_stdout=(partition=='iliad'))
    # print('gpu_info: ', gpu_info)
    # print('time_in_sec', time_in_sec)
    gpu_config = aggregate_gpu_info(gpu_info)
    print('overall gpu_config: ', gpu_config)
    # print('time_in_sec: ', time_in_sec)
    return {
            'state': state,
            'gpu_config': gpu_config,
            'lab_partition': lab_partition
            }


def parse_sinfo_response(response:
        List[Sequence[Any]]
        ) -> Dict[str, Sequence[bytes]]:
    """
    Parse and process slurm sinfo response.

    Args:
        Response: Nested list of bytes.
    Return:
        A processed dict of <str, Any>.
    """
    keys = response[0].split(b' ')

    # split response into column+row nested list
    response_array = [response[i].split(b' ') for i in range(len(response))]
    # remove first stream spec first line and last line artifact
    response_array_clean = response_array[1:-1]
    lab_partition = {}
    lab_partition_nodes = {}
    names_map = {}
    total_gpu_info = []
    total_node_num = []
    for line in response_array_clean:
        # TODO(jaredquincy): clean up this manual syntax.
        # somewhat manual splitting of the lab names to remove special chars
        full_partition_name = line[SINFO_KEY_INDEX['partition']]
        partition_name = full_partition_name.split(b'-')[0]
        partition_name = partition_name.split(b'*')[0]
        # check duplicate partition condition
        if partition_name in names_map.keys():
            if names_map[partition_name] != full_partition_name:
                continue
        else:
            suffix = full_partition_name.split(b'-')
            if len(suffix) >= 2:
                suffix = suffix[1]
                if suffix != b'cpu':
                    names_map[partition_name] = full_partition_name
            else:
                names_map[partition_name] = full_partition_name
        gpu_request = line[SINFO_KEY_INDEX['gpus']]
        gpu_request = gpu_request.split(b',')
        node_list = line[SINFO_KEY_INDEX['node_list']].decode("utf-8").split(',')
        num_nodes = [interpret_node_list(node_list)] * len(gpu_request)
        gpu_info = [parse_gpu_info_string(
            gpu_request[i],
            job_time=0
        ) for i in range(len(gpu_request))]
        total_gpu_info += gpu_info
        total_node_num += num_nodes
        if partition_name not in lab_partition.keys():
            lab_partition[partition_name] = gpu_request
            lab_partition_nodes[partition_name] = num_nodes
        else:
            lab_partition[partition_name] += gpu_request
            lab_partition_nodes[partition_name] += num_nodes
    #  post process
    for partition, gpu_details in lab_partition.items():
        lab_partition[partition] = [
                parse_gpu_info_string(
                    gpu_details[i], 0
                    )
                for i in range(len(gpu_details))
                ]
        lab_partition[partition] = aggregate_gpu_info(
               lab_partition[partition],
               print_stdout=(partition=='iliad'),
               lab_partition_nodes=lab_partition_nodes[partition]
               )
    total_gpu_config = aggregate_gpu_info(total_gpu_info, lab_partition_nodes=total_node_num)
    print('total gpu config: ', total_gpu_config)
    return lab_partition


def aggregate_gpu_info(
        gpu_info: List[List[Any]],
        print_stdout: bool = False,
        lab_partition_nodes: Any = None) -> Dict[str, Any]:
    """
    Aggregate job-level GPU requests by type.

    Args:
        gpu_info: list of parsed gpu info strings.
    Return:
        dictionary with GPU request data aggregated by resource type.

    """
    if lab_partition_nodes is not None:
        num_nodes = lab_partition_nodes
    else:
        num_nodes = [1] * len(gpu_info)
    gpu_config = {'total': 0, 'pending': 0, 'running': 0}
    for i in range(len(gpu_info)):
        gpu_spec = gpu_info[i]
        gres_type = gpu_spec['gres_type']
        num_gpus = gpu_spec['num_gres'] * num_nodes[i]
        job_time = gpu_spec['time']
        if gres_type not in gpu_config:
            gpu_config[gres_type] = num_gpus
        else:
            gpu_config[gres_type] += num_gpus
        gpu_config['total'] += num_gpus
        if print_stdout:
            print('num_gpu: {}, job_time: {}'.format(num_gpus, job_time))
        if job_time > 0:
            gpu_config['running'] += num_gpus
        else:
            gpu_config['pending'] += num_gpus
    return gpu_config


def safe_cast_byte_to_int(byte_string: bytes) -> int:
    """
    Turn byte sequence to int.

    Args:
        byte_string: byte sequence.
    Return:
        casted to int, unless null -> 0
    """
    if byte_string == b'(null)':
        return 0
    else:
        return int(byte_string)

def parse_gpu_info_string(
        gpu_byte_string: List[bytes],
        job_time: float,
        ) -> gpuSpec:
    """
    Parse GPU info string.

    Args:
        gpu_byte_string: byte string structured 'gres:(optional:type):num_gpus'
        job_time: job time associated with this gpu request
    Return:
        gpuSpec object
    """
    separated_gpu_info = gpu_byte_string.split(b':')
    if len(separated_gpu_info) == 3:
        gres, gres_type, num_gres = separated_gpu_info
    elif len(separated_gpu_info) == 2:
        gres, num_gres = separated_gpu_info
        gres_type = b'unspecified'
    else:
        gres = b'(null)'
        gres_type = b'unspecified'
        num_gres = 0

    gpu_info = {
            'gres':gres.decode("utf-8"),
            'gres_type': gres_type.decode("utf-8"),
            'num_gres': safe_cast_byte_to_int(num_gres),
            'time': job_time,
                }
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
    if len(separated_time_info) == 3:
        day_hour, minute, second = separated_time_info
    else:
        minute, second = separated_time_info
        day_hour = b'0-0'
    day_hour = day_hour.split(b'-')
    if len(day_hour) == 2:
        day, hour = day_hour
    else:
        day, hour = 0, day_hour[0]
    day = float(day) * DAY_CONV
    hour = float(hour) * HOUR_CONV
    minute = float(minute) * MINUTE_CONV
    second = float(second)
    time_in_sec = day + hour + minute + second
    return time_in_sec


def main():
    proceed = True
    while proceed:
        # queue state parsing
        process = subprocess.run(['squeue -o "%A %P %M %b"'],
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE)
        processed_output = process.stdout
        line_parsed_processed_output = processed_output.split(b'\n')
        squeue_state = parse_slurm_squeue_response(line_parsed_processed_output)
        # sinfo state parsing
        sinfo = subprocess.run(['sinfo -o "%P %M %G %N"'],
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE)
        sinfo_stdout = sinfo.stdout
        line_parsed_sinfo_stdout = sinfo_stdout.split(b'\n')
        sinfo_parsed = parse_sinfo_response(line_parsed_sinfo_stdout)
        squeue_lab_partition = squeue_state['lab_partition']
        for partition in squeue_lab_partition:
            total_gpu_supply = sinfo_parsed[partition]['total']
            total_requested_gpus = squeue_lab_partition[partition]['total']
            slack = total_gpu_supply - total_requested_gpus
            overflow = total_requested_gpus - total_gpu_supply
            squeue_lab_partition[partition]['slack'] = slack
            squeue_lab_partition[partition]['supply'] = total_gpu_supply
            squeue_lab_partition[partition]['overflow'] = overflow
        squeue_lab_partition = OrderedDict(sorted(squeue_lab_partition.items(),
            key=lambda x: x[1]['supply'],
            reverse=True))
        sinfo_parsed = OrderedDict(sorted(sinfo_parsed.items(),
            key=lambda x: x[1]['total'], reverse=True))
        print('-----------')
        total_gpus = 0
        total_running_gpus = 0
        total_pending_gpus = 0
        for partition in squeue_lab_partition:
            if partition in [b'tibet', b'tibet*']:
                continue
            print('partition: {} | total: {} | slack: {} | pending: {} | running: {}'.format(
                partition,
                squeue_lab_partition[partition]['supply'],
                squeue_lab_partition[partition]['slack'],
                squeue_lab_partition[partition]['pending'],
                squeue_lab_partition[partition]['running']
               ))
            total_gpus += squeue_lab_partition[partition]['supply']
            total_running_gpus += squeue_lab_partition[partition]['running']
            total_pending_gpus += squeue_lab_partition[partition]['pending']
        total_gpus_real  = 0
        for partition in sinfo_parsed.keys():
            total_gpus_real += sinfo_parsed[partition]['total']
        print('------------')
        print('total_gpus: ', total_gpus_real)
        print('total_pending_gpus: ', total_pending_gpus)
        print('total_running_gpus: ', total_running_gpus)
        print('total_running+pending_gpus: ', total_pending_gpus+total_running_gpus)
        print('sinfo[jag] compute spec: ', sinfo_parsed[b'jag'])
        # time.sleep(50000.0)
        proceed=False

main()
