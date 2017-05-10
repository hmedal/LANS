'''
Created on Feb 7, 2017

@author: tb2038
'''

from __future__ import division
import numpy as np
import pandas as pd
import csv
import sys
import os
import warnings
warnings.filterwarnings("ignore")

nRole = 8
noOfBins = 200

Data_file = sys.argv[1]
Role_info = sys.argv[2]
output_filename = sys.argv[3]
output_nodehist = sys.argv[4]

if os.path.exists(output_filename):
    try:
        os.remove(output_filename)
    except OSError:
        pass

if os.path.exists(output_nodehist):
    try:
        os.remove(output_nodehist)
    except OSError:
        pass

'''
Data_file = "CTU13_5_Sample.csv"
Role_info = "Roles_information.csv"
output_filename="Param_Roles_Information.csv"
output_nodehist = 'node_degree_histogram2.txt'
'''
Role_df = pd.read_csv(Role_info, delimiter=',', usecols=[0, 1])
Data_file_df = pd.read_csv(Data_file, delimiter=',',usecols=[0, 3, 6])

#print Role_df['Node'][55]

s = (nRole, nRole)
All_Role_Matrix = dict()
Edge_matrix = np.zeros(s)
nNodes = []

for i in range(nRole):
    nodeList = []
    for j in range(len(Role_df)):
        if Role_df['Role'][j] == i:
            nodeList.append(Role_df['Node'][j])
    nNodes.append(len(nodeList))
    
    s = (nRole, nRole)
    RoleArray = np.zeros(s)
    for node in nodeList:
        node_data = Data_file_df[Data_file_df['SrcAddr']== node]
        for r in range(1, len(node_data)):
            #find the column state of role matrix
            node_column = node_data['DstAddr'].iloc[r]
            node_role_column = Role_df[Role_df['Node'] == node_column]
            Column_state = node_role_column.iloc[0, 1]

            #find the row state of role matrix
            node_row = node_data['DstAddr'].iloc[r-1]
            node_role_row = Role_df[Role_df['Node'] == node_row]
            row_state = node_role_row.iloc[0, 1]
            try:
                RoleArray[row_state][Column_state] = RoleArray[row_state][Column_state] + 1
            except:
                print "row_state:", row_state
                print "Column_state:", Column_state
                pass

######Calculating edge proportions
####################################################################
        for row in range(len(node_data)):
            node_column_edge = node_data['DstAddr'].iloc[row]
            node_role_column_edge = Role_df[Role_df['Node'] == node_column_edge]
            Column_state_edge = node_role_column_edge.iloc[0, 1]

            node_role_row_edge = Role_df[Role_df['Node'] == node]
            row_state_edge = node_role_row_edge.iloc[0, 1]
            try:
                Edge_matrix[row_state_edge][Column_state_edge] = Edge_matrix[row_state_edge][Column_state_edge] + 1
            except:
                print "row_state_edge: ",row_state_edge
                print "Column_state_edge: ",Column_state_edge
                pass
    #Store the RoleArray of role 1 in the Role1_matrix_count
    #print "RoleArray", RoleArray
    All_Role_Matrix[i] = RoleArray[:]

for i in range(nRole):
    All_Role_Matrix[i] = All_Role_Matrix[i]/All_Role_Matrix[i].sum(axis = 1) [:, None]
    All_Role_Matrix[i] = np.nan_to_num(All_Role_Matrix[i])
    # Replace the nan in an element of numpy array by zero

Edge_matrix_prob = Edge_matrix[:]
Matrix_sum = np.sum(Edge_matrix_prob)
Edge_matrix_prob = Edge_matrix_prob/Matrix_sum
Edge_matrix_prob = np.nan_to_num(Edge_matrix_prob)

matrices_f= open(output_filename,'ab')

np.savetxt(matrices_f, np.array(nRole)[np.newaxis],fmt="%5.1f",  delimiter=",")
np.savetxt(matrices_f, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")

for n in range(nRole):
    np.savetxt(matrices_f, All_Role_Matrix[n], fmt="%5.3f", delimiter=",")
    np.savetxt(matrices_f, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")
        
np.savetxt(matrices_f, Edge_matrix,  fmt="%5.1f", delimiter=",")
np.savetxt(matrices_f, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")
np.savetxt(matrices_f, Edge_matrix_prob,  fmt="%5.3f", delimiter=",")

np.savetxt(matrices_f, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")

np.savetxt(matrices_f, np.array(nNodes),fmt="%5.1f",  delimiter=",")
np.savetxt(matrices_f, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")

np.savetxt(matrices_f, np.array(Matrix_sum)[np.newaxis],fmt="%5.1f",  delimiter=",")



Data_file_df = pd.read_csv(Data_file, delimiter=',', usecols=[3, 6])
Role_df = pd.read_csv(Role_info, delimiter=',', usecols=[0, 1])

f = open(output_nodehist,'a')
f.write("%s\n" %noOfBins)

for i in range(nRole):
    nodeList = Role_df[Role_df['Role'] == i]['Node'].values.tolist()
   
    if len(nodeList) > 0:
        Indegree_Lst = []
        Outdegree_Lst = []
        Indegree_count = 0
        Outdegree_count = 0
        
        for node in nodeList:
            Indegree_df = Data_file_df[Data_file_df['DstAddr'] == node]
            Outdegree_df = Data_file_df[Data_file_df['SrcAddr'] == node]
            Indegree_count = len(Indegree_df)
            Outdegree_count = len(Outdegree_df)
            Indegree_Lst.append(Indegree_count)
            Outdegree_Lst.append(Outdegree_count)
            
        hist1_2, bins_indegree, bins_outdegree = np.histogram2d(Indegree_Lst, Outdegree_Lst, noOfBins)
    
        for i in range(noOfBins):
            for j in range(noOfBins):
                f.write("%s, %s, %s \n" % (bins_indegree[i],bins_outdegree[j],hist1_2[i,j]))
        #f.write("----------------------------------------------------------------------------------\n")

f.close()
