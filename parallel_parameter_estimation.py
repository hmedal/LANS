'''
Created on Feb 7, 2017

@author: tb2038
'''

from __future__ import division
from mpi4py import MPI
import numpy as np
import pandas as pd
import csv
import sys
import os
import warnings
from os import listdir

warnings.filterwarnings("ignore")

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
mpi_size = comm.Get_size()

nRole = 4
noOfBins = 200

def find_all_filenames(path_to_dir):
    filenames = listdir(path_to_dir)
    return filenames

conf_file = open ( 'Configuration.txt',"r" )

lineList = conf_file.readlines()
conf_file.close()
input_folder = lineList[-1]+"/input_files/"

allFiles = find_all_filenames(input_folder)
temp_folder = os.path.dirname(os.path.dirname(input_folder))
temp_folder += "/temp/"

for f in allFiles:
    Data_file = input_folder+f
    Role_info = temp_folder +"role_information"+ f.split('.')[0] +".csv"
    output_filename = temp_folder +"Param_Roles_Information"+ f.split('.')[0] +".csv"
    output_nodehist = temp_folder +"node_degree_histogram2"+ f.split('.')[0] +".txt"

    Role_df = pd.read_csv(Role_info, delimiter=',', usecols=[0, 1])
    Data_file_df = pd.read_csv(Data_file, delimiter=',',usecols=[0, 3, 6])

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

    s = (nRole, nRole)
    All_Role_Matrix = dict()
    Edge_matrix = np.zeros(s)
    nNodes = []

    i = rank
    if rank==0:
        totals_Edge_matrix = np.zeros(s)
    else:
        totals_Edge_matrix = None
    #----------------- Calculation of transition probability matrices -----------------#
    nodeList = []
    for j in range(len(Role_df)):
        if Role_df['Role'][j] == i:
            nodeList.append(Role_df['Node'][j])
    len_nNodes = len(nodeList)

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
                print("row_state:", row_state)
                print("Column_state:", Column_state)
                pass

    ################################ Calculating edge proportions ###################################
        for row in range(len(node_data)):
            node_column_edge = node_data['DstAddr'].iloc[row]
            node_role_column_edge = Role_df[Role_df['Node'] == node_column_edge]
            Column_state_edge = node_role_column_edge.iloc[0, 1]

            node_role_row_edge = Role_df[Role_df['Node'] == node]
            row_state_edge = node_role_row_edge.iloc[0, 1]
            try:
                Edge_matrix[row_state_edge][Column_state_edge] = Edge_matrix[row_state_edge][Column_state_edge] + 1
            except:
                print("row_state_edge: ",row_state_edge)
                print("Column_state_edge: ",Column_state_edge)
                pass


    #----------------------- Calculation of node histogram  -----------------#
    h_nodeList = Role_df[Role_df['Role'] == i]['Node'].values.tolist()
    s1 = (noOfBins,noOfBins)
    hist1_2 = np.zeros(s1)
    bins_indegree = []
    bins_outdegree = []
    if len(h_nodeList) > 0:
        Indegree_Lst = []
        Outdegree_Lst = []
        Indegree_count = 0
        Outdegree_count = 0

        for node in h_nodeList:
            Indegree_df = Data_file_df[Data_file_df['DstAddr'] == node]
            Outdegree_df = Data_file_df[Data_file_df['SrcAddr'] == node]
            Indegree_count = len(Indegree_df)
            Outdegree_count = len(Outdegree_df)
            Indegree_Lst.append(Indegree_count)
            Outdegree_Lst.append(Outdegree_count)

        hist1_2, bins_indegree, bins_outdegree = np.histogram2d(Indegree_Lst, Outdegree_Lst, noOfBins)


    #----------------------- Gather all data -------------------------#

    comm.Reduce([Edge_matrix, MPI.DOUBLE], [totals_Edge_matrix, MPI.DOUBLE], op = MPI.SUM, root = 0)
    All_Role_Matrix = comm.gather(RoleArray, root=0)
    All_hist1_2 = comm.gather(hist1_2, root=0)
    All_bins_indegree = comm.gather(bins_indegree, root=0)
    All_bins_outdegree = comm.gather(bins_outdegree, root=0)
    nNodes = comm.gather(len_nNodes, root=0)
    comm.barrier()

    #These codes are executing for each file
    if rank == 0:
        for i in range(nRole):
            All_Role_Matrix[i] = All_Role_Matrix[i]/All_Role_Matrix[i].sum(axis = 1) [:, None]
            All_Role_Matrix[i] = np.nan_to_num(All_Role_Matrix[i])

        matrices_file = open(output_filename,'ab')

        np.savetxt(matrices_file, np.array(nRole)[np.newaxis],fmt="%5.1f",  delimiter=",")
        np.savetxt(matrices_file, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")

        for n in range(nRole):
            np.savetxt(matrices_file, All_Role_Matrix[n], fmt="%5.3f", delimiter=",")
            np.savetxt(matrices_file, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")

        Edge_matrix_prob = totals_Edge_matrix[:]
        Matrix_sum = np.sum(Edge_matrix_prob)
        Edge_matrix_prob = Edge_matrix_prob/Matrix_sum
        Edge_matrix_prob = np.nan_to_num(Edge_matrix_prob)

        np.savetxt(matrices_file, totals_Edge_matrix,  fmt="%5.1f", delimiter=",")
        np.savetxt(matrices_file, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")
        np.savetxt(matrices_file, Edge_matrix_prob,  fmt="%5.3f", delimiter=",")

        np.savetxt(matrices_file, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")

        np.savetxt(matrices_file, np.array(nNodes[0:nRole]),fmt="%5.1f",  delimiter=",")
        np.savetxt(matrices_file, np.array([-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99,-99])[np.newaxis], fmt="%5.1f", delimiter=",")

        np.savetxt(matrices_file, np.array(Matrix_sum)[np.newaxis],fmt="%5.1f",  delimiter=",")
        matrices_file.close()

        #--------------------------------------------------------------------------------------------#
        nodehist_file = open(output_nodehist,'a')
        nodehist_file.write("%s\n" %(noOfBins+1))
        for n in range(nRole):
            a = All_bins_indegree[n]
            b = All_bins_outdegree[n]
            c = All_hist1_2[n]
            for i in range(0, noOfBins):
                for j in range(0, noOfBins):
                    if len(a)>0 and len(b)>0 and len(c)>0:
                        nodehist_file.write("%s, %s, %s \n" % (a[i], b[j], c[i,j]))
                        if j == noOfBins-1:
                            nodehist_file.write("%s, %s, %s \n" % (a[i], b[j+1], 0.0))
                if i == noOfBins-1:
                    for j in range(0, (noOfBins+1)):
                        nodehist_file.write("%s, %s, %s \n" % (a[i+1], b[j], 0.0))

        nodehist_file.close()