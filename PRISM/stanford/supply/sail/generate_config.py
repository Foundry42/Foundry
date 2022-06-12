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
    print('query_response: ', query_response)

    # Per line in query_response, accumulate info into dict for config
    # cleaning query response to remove keys and end token
    iterable_query = query_response[1:-1]
    for line in iterable_query:
        #TODO(jq): get rid of hardcoded keys
        partition_name = line[0]
        gres = line[1]
        node_list = line[2]

        # parse_gres
        parse_gres(gres)

        # parse node_list
        interpret_node_list(node_list)

def parse_gres(gres: bytes = None) -> List[bytes]:
    print(gres)




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
    print('node_list: ', node_list)
    print('num nodes for node_list: ', num_nodes)
    return num_nodes




parse_parition_gpu_info('iris')
