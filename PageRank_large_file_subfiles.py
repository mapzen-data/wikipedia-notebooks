

import networkx
import PageRank
import json
import operator
import sys
import glob

from all_functions import *


input_path = sys.argv[1]
output_path = sys.argv[2]

allFiles = glob.glob(input_path + "\*.json")
d={}
link_dictionary = {}
x=0
for file_ in allFiles:
    with open(file_, 'r') as fp:
        link_dictionary[x] = json.load(fp)
        x+=1
with open(input_path, 'r') as fp:
    link_dictionary = json.load(fp)


reversed_link_dictionary = []
for pointed_to, points_to in link_dictionary.iteritems():
    for node in points_to:
        reversed_link_dictionary.append((node, pointed_to))


network = networkx.DiGraph(reversed_link_dictionary)


Page_rank_network=networkx.algorithms.pagerank(network)
keys_interest=set(link_dictionary.keys())


PageRank_nodes={}
for key, value in Page_rank_network.iteritems():
    if key in keys_interest:
        PageRank_nodes.update({key:value}) 


with open(output_path, 'w') as outfile:
    json.dump(PageRank_nodes, outfile)
