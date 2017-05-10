__author__ = 'fz56@msstate.edu'

# The input graph format is .CSV
# Users can specify which columns are source IP and Destination IP
# Calculate graph properties by vertex
# Write graph properties into CSV file

import networkx as nx
import csv
import sys

#print sys.version
input_fname = sys.argv[1]
output_fname = sys.argv[2]

edgeList =[]
with open(input_fname) as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',', lineterminator='\n')
    for row in readCSV:
        edgeList.append((row[3], row[6]))
edgeList[:] = [value for value in edgeList if value[0] != value[1]]
G = nx.DiGraph() # create directed graph (nx.DiGraph()) or undirected graph (nx.Graph())
G.add_edges_from(edgeList)
for n in G.nodes():
    G.node[n]["label"] = n

node_Attributes = nx.get_node_attributes(G, "label")  # label or name. Please check graph file before set this attribute
#print("node_Attributes")

if nx.is_directed(G):
    nodeIndegree = G.in_degree()
    #print("nodeIndegree")

    nodeOutdegree = G.out_degree()
    #print("nodeOutdegree")

    nodeInDegreeCentrality = nx.in_degree_centrality(G)
    #print("nodeInDegreeCentrality")

    nodeOutDegreeCentrality = nx.out_degree_centrality(G)
    #print("nodeOutDegreeCentrality")
else:

    nodeDegree = nx.degree(G)
    #print("nodeDegree")

    clusteringCoefficient = nx.clustering(G)
    #print("clusteringCoefficient")

    triangles = nx.triangles(G)
    #print("triangles")

averageNeighborDegree = nx.average_neighbor_degree(G)
#print("averageNeighborDegree")

# eigenVectorCentrality = nx.eigenvector_centrality(G)
# # print(eigenVectorCentrality)

pageRank = nx.pagerank(G)
#print("pageRank")

#harmonicCentrality = nx.harmonic_centrality(G)
#print("harmonicCentrality")

#betweennessCentrality = nx.betweenness_centrality(G)
#print("betweennessCentrality")

#closenessCentrality = nx.closeness_centrality(G)
#print("closenessCentrality")

coreNumber = nx.core_number(G)
#print("coreNumber")

property = {}

if nx.is_directed(G):
    for key in nodeIndegree.keys():
        property.setdefault(key, []).append(node_Attributes[key])
        property.setdefault(key, []).append(nodeIndegree[key])
        property.setdefault(key, []).append(nodeOutdegree[key])
        property.setdefault(key, []).append(averageNeighborDegree[key])
        property.setdefault(key, []).append(nodeInDegreeCentrality[key])
        property.setdefault(key, []).append(nodeOutDegreeCentrality[key])
        property.setdefault(key, []).append(pageRank[key])
        #property.setdefault(key, []).append(harmonicCentrality[key])
        #property.setdefault(key, []).append(betweennessCentrality[key])
        #property.setdefault(key, []).append(closenessCentrality[key])
        property.setdefault(key, []).append(coreNumber[key])
    #print("property")
    with open(output_fname, 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', lineterminator='\n')
        writer.writerow(["vertexID",
                         "nodeInDegree",
                         "nodeOutDegree",
                         "averageNeighborDegree",
                         "nodeInDegreeCentrality",
                         "nodeOutDegreeCentrality",
                         "pageRank",
                         #"harmonicCentrality",
                         #"betweennessCentrality",
                         #"closenessCentrality",
                         "coreNumber"
                         ])

        for k in property.keys():
            writer.writerow(property[k])
else:

    for key in nodeDegree.keys():
        property.setdefault(key, []).append(node_Attributes[key])
        property.setdefault(key, []).append(nodeDegree[key])
        property.setdefault(key, []).append(averageNeighborDegree[key])
        property.setdefault(key, []).append(clusteringCoefficient[key])
        property.setdefault(key, []).append(pageRank[key])
        property.setdefault(key, []).append(triangles[key])
        #property.setdefault(key, []).append(harmonicCentrality[key])
        #property.setdefault(key, []).append(betweennessCentrality[key])
        #property.setdefault(key, []).append(closenessCentrality[key])
        property.setdefault(key, []).append(coreNumber[key])
    #print("property")
    with open(output_fname, 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', lineterminator='\n')
        writer.writerow(["vertexID",
                         "nodeDegree",
                         "averageNeighborDegree",
                         "clusteringCoefficient",
                         "pageRank",
                         "triangles",
                         #"harmonicCentrality",
                         #"betweennessCentrality",
                         #"closenessCentrality",
                         "coreNumber"
                         ])

        for k in property.keys():
            writer.writerow(property[k])
