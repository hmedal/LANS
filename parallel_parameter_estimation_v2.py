'''
Created on Feb 7, 2017

@author: sh2364, tb2038
'''

from __future__ import division
from mpi4py import MPI
import numpy as np
import pandas as pd
import csv
import sys
import os
import warnings
import Read_Params as rp
from os import listdir

warnings.filterwarnings("ignore")

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
numprocs = comm.Get_size()

params = rp.Read_Params().Params
nRole = params['nRole']
noOfBins = params['noBin']

def find_all_filenames(path_to_dir):
    filenames = listdir(path_to_dir)
    return filenames

conf_file = open('Configuration.txt',"r")
lineList = conf_file.readlines()
conf_file.close()
input_folder = lineList[-1]+"/input_files/"
allFiles = find_all_filenames(input_folder)
temp_folder = os.path.dirname(os.path.dirname(input_folder))
temp_folder += "/temp/"

#debug_filename = temp_folder + "output_"+str(rank)+".txt"
#outputstring = ""

for f in allFiles:
    #text_file = open(debug_filename, "a")
    Data_file = input_folder+f
    Role_info = temp_folder +"role_information"+ f.split('.')[0] +".csv"
    Role_df = pd.read_csv(Role_info, delimiter=',', usecols=[0, 1])
    Data_file_df = pd.read_csv(Data_file, delimiter=',',usecols=[0, 3, 6])
    s = (nRole, nRole)
    if rank == 0:
        #Initialize per file
        All_Role_Matrix = dict()
        All_hist1_2 = dict()
        All_bins_indegree = dict()
        All_bins_outdegree = dict()
        totals_Edge_matrix = np.zeros(s)
        nNodes = []

    for i in range(nRole):
        if rank == 0:
            sum_Edge_matrix = np.zeros(s)
            sum_RoleArray = np.zeros(s)
            merge_Indegree_Lst = []
            merge_Outdegree_Lst = []
            nodeList = Role_df.loc[Role_df['Role'] == i, "Node"].tolist()
            nNodes.append(len(nodeList))
            #outputstring = "len(nodeList): " + str(len(nodeList))+"\n"
            #text_file.write("%s" % outputstring)
        else:
            nodeList = None
            sum_Edge_matrix = None
            sum_RoleArray = None
            merge_Indegree_Lst = None
            merge_Outdegree_Lst = None

        Edge_matrix = np.zeros(s)
        RoleArray = np.zeros(s)
        nodeList = comm.bcast(nodeList, root=0)
        #print "No of nodes in role: "+str(i)+ " is "+str(len(nodeList))
        #outputstring = "Found nodeList"+"\n"
        #text_file.write("%s" % outputstring)
        noRow = int(np.ceil(len(nodeList)/float(numprocs)))
        start = rank*noRow
        end = start+noRow
        if start > len(nodeList):
            start = len(nodeList)
        if end > len(nodeList):
            end = len(nodeList)

        #outputstring = "start: "+str(start)+", End: "+str(end)+ "\n"
        #text_file.write("%s" % outputstring)
        Indegree_Lst = []
        Outdegree_Lst = []
        #print "rank: "+str(rank)+" , start: "+str(start)+" , end: "+str(end)
        for node in nodeList[start:end]:
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
                    print("Rank: ",rank,", row_state:", row_state)
                    print("Rank: ",rank,", Column_state:", Column_state)
                    pass

            #outputstring = "After calculating RoleArray"+"\n"
            #text_file.write("%s" % outputstring)
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
                    print("Rank: ",rank,", row_state_edge: ",row_state_edge)
                    print("Rank: ",rank,", Column_state_edge: ",Column_state_edge)
                    pass

            Indegree_df = Data_file_df[Data_file_df['DstAddr'] == node]
            Outdegree_df = Data_file_df[Data_file_df['SrcAddr'] == node]
            Indegree_count = len(Indegree_df)
            Outdegree_count = len(Outdegree_df)
            Indegree_Lst.append(Indegree_count)
            Outdegree_Lst.append(Outdegree_count)

        #Merge all work for single role
        #outputstring = "Finish calculation, now start merging for single role"+"\n"
        #text_file.write("%s" % outputstring)
        comm.Reduce([RoleArray, MPI.DOUBLE], [sum_RoleArray, MPI.DOUBLE],op = MPI.SUM, root=0)
        comm.Reduce([Edge_matrix, MPI.DOUBLE], [sum_Edge_matrix, MPI.DOUBLE],op = MPI.SUM, root=0)
        merge_Indegree_Lst = comm.gather(Indegree_Lst, root=0)
        merge_Outdegree_Lst = comm.gather(Outdegree_Lst, root=0)

        if rank==0:
            merge_Indegree_Lst = [val for sublist in merge_Indegree_Lst for val in sublist]
            merge_Outdegree_Lst = [val for sublist in merge_Outdegree_Lst for val in sublist]

        #comm.barrier()
        #print "Rank: "+str(rank)+" >> After barrier at line 148."
        #outputstring = "sum_RoleArray: "+"\n"+ str(sum_RoleArray) +"\n"
        #outputstring = "Rank: "+str(rank)+" >> After barrier at line 148. " +"\n"
        #text_file.write("%s" % outputstring)
        #outputstring = "sum_Edge_matrix: "+"\n"+ str(sum_Edge_matrix) +"\n"
        #text_file.write("%s" % outputstring)

        if rank == 0:
            #print "Rank: "+str(rank)+" >> Merged all work for role: "+str(i)
            #outputstring = "Rank 0 >> Merged all work for role: "+ str(i) +"\n"
            #text_file.write("%s" % outputstring)
            s1 = (noOfBins,noOfBins)
            hist1_2 = np.zeros(s1)
            bins_indegree = []
            bins_outdegree = []
            #print "merge_Indegree_Lst: ", merge_Indegree_Lst
            #print "merge_Outdegree_Lst: ", merge_Outdegree_Lst
            #outputstring = "merge_Indegree_Lst: " + "\n"
            #text_file.write("%s" % outputstring)
            #text_file.write(merge_Indegree_Lst)
            #outputstring = "\n" + "merge_Outdegree_Lst: " + "\n"
            #text_file.write("%s" % outputstring)
            #text_file.write(merge_Outdegree_Lst)
            #outputstring = "Rank: "+str(rank)+" >> Before calculating histogram. " +"\n"
            hist1_2, bins_indegree, bins_outdegree = np.histogram2d(merge_Indegree_Lst, merge_Outdegree_Lst, noOfBins)

            #Save all work for role i
            All_Role_Matrix[i] = sum_RoleArray
            All_hist1_2[i] = hist1_2
            All_bins_indegree[i] = bins_indegree
            All_bins_outdegree[i] = bins_outdegree
            nNodes[i] = len(nodeList)

            #Sum the edges across all roles
            totals_Edge_matrix = totals_Edge_matrix+sum_Edge_matrix

        comm.barrier()#All processor must unite at this point, then go forward for next role
        #outputstring = "Rank: "+str(rank)+" >> After barrier at line 183. " +"\n"
        #text_file.write("%s" % outputstring)
    #Write down everything for file f
    if rank == 0:
        #outputstring = "Rank 0 >> Merged all work for all role: "+"\n"
        #text_file.write("%s" % outputstring)
        output_filename = temp_folder +"Param_Roles_Information"+ f.split('.')[0] +".csv"
        output_nodehist = temp_folder +"node_degree_histogram2"+ f.split('.')[0] +".txt"

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

    #text_file.close()