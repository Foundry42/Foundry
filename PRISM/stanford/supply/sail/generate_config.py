import subprocess
import time
from typing import Any, Generic, Optional, Dict, List
from collections import OrderedDict

######
# UTILS - move to separate file once this grows
######

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

######
# UTILS
######
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

    # Per line in query_response, accumulate info into dict for config
    # cleaning query response to remove keys and end token
    gres_info_list = []
    gres_info = {}
    gres_info['total_gres'] = 0
    gres_info['partition'] = partition

    iterable_query = query_response[1:-1]
    for line in iterable_query:
        #TODO(jq): get rid of hardcoded keys
        partition_name = line[0]
        gres_list = line[1].split(b',')
        node_list = line[2]

        # parse and process gres list
        for gres in gres_list:
            gres_dict = parse_gres_sinfo(gres)
            # parse node_list
            node_num = interpret_node_list(node_list)
            gres_dict['total_gres'] = gres_dict['num_gres'] * node_num
            gres_info_list.append(gres_dict)
            gres_info['total_gres'] += gres_dict['total_gres']


    gres_info['partition_details'] = gres_info_list

    print('gres info: ', gres_info)
    return gres_info

def parse_gres_sinfo(
        gres: bytes,
        ) -> Dict[Any, Any]:
    """
    Parse GPU info string.

    Args:
        gpu_byte_string: byte string structured 'gres:(optional:type):num_gpus'
        job_time: job time associated with this gpu request
    Return:
        gpuSpec object
    """
    # split gres list by gpu_type/gpu_box
    gres_nodes = gres.split(b',')

    separated_gpu_info = gres.split(b':')
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
            'num_gres': safe_cast_byte_to_int(num_gres)
            }
    return gpu_info



def interpret_node_list(
        node_list: bytes,
        ) -> Dict[Any, Any]:
    """
    Parse list of nodes and return the number of node subclusters.

    Args:
        interim_node_rep:  list of nodes where input string was parsed by ','
    Returns:
        number of node subclusters and other node details.
    """
    num_nodes = 0
    # split node list by comma
    node_list = node_list.decode("utf-8").split(',')

    # by subnode, extract node count by reading ranges
    for node in node_list:
        dash_split = node.split('-')
        if len(dash_split) == 1:
            num_nodes += 1
        elif len(dash_split) == 2: # split by dash
            # if numbers, convert, subtract and return
            a = dash_split[0][-1] # grab last character of first
            x = dash_split[0][-2:] # grab last 2 in case 10-99
            m = dash_split[0][-3:] # grab last 3 in case 100-999
            q = dash_split[0][-4:] # grab last 4 in case 1000-9999

            b = dash_split[-1][0] # grab first character of last
            y = dash_split[-1][0:2].split(']')[0] # grab first two in case 10-99
            n = dash_split[-1][0:3].split(']')[0] # grab first three in case 100-999
            s = dash_split[-1][0:4].split(']')[0] # grab first four in case 100-999

            # if all  char values, return
            if a.isnumeric() and b.isnumeric:
                # check first num in range
                if q.isnumeric():
                    first_num = int(q)
                elif m.isnumeric():
                    first_num = int(m)
                elif x.isnumeric():
                    first_num = int(x)
                else:
                    first_num = int(a)

                # check second num in range
                if s.isnumeric():
                    second_num = int(s)
                elif n.isnumeric():
                    second_num = int(n)
                elif y.isnumeric():
                    second_num = int(y)
                else:
                    second_num = int(b)
                num_nodes += (second_num - first_num + 1)
            else:
                num_nodes += 1
        else:
            num_nodes += 1
    print('node_list: ', node_list)
    print('num nodes for node_list: ', num_nodes)
    return num_nodes




parse_parition_gpu_info('iris')
parse_parition_gpu_info('sphinx')
parse_parition_gpu_info('jag-lo')
