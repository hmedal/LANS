import pandas as pd
import numpy as np
import sys
import re
import warnings
import os
import itertools
from os import listdir
import mpi4py
from mpi4py import MPI
warnings.simplefilter('ignore', category=FutureWarning)

## Initialize multithreading
comm = mpi4py.MPI.COMM_WORLD
w = comm.Get_rank()
w = int(w)

def find_csv_filenames(path_to_dir):
    filenames = listdir(path_to_dir)
    return filenames

## Uncomment these lines for Shadow submission
conf_file = open ( 'Configuration.txt',"r" )
lineList = conf_file.readlines()
conf_file.close()
filePrefix = lineList[-1]+"/input_files/"

## Comment out this line for Shadow submission (testing purposes only)
#filePrefix = "C:/Users/Jonathan/Dropbox/HPDA_Simulation_Project/code/Edge attributes/Test Files/input_files/"

## Get a list of input files from the /temp/ folder
ctu_files = find_csv_filenames(filePrefix)
temp_folder = os.path.dirname(os.path.dirname(filePrefix))
temp_folder += "/temp/"

## Create identifiers to find role information files
role_files = []
for f in ctu_files:
	role_files.append(f)
i = 0
for f in role_files:
	role_files[i] = 'role_information' + "".join(itertools.takewhile(str.isdigit, f)) + '.csv'
	i = i + 1

## Exit if not enough processors were assigned for the number of input files
if w >= len(ctu_files):
	sys.exit(0)

## Read input files into pandas (pd) for manipulation
ctu_pd = pd.read_csv(filePrefix + ctu_files[w])
roles_pd = pd.read_csv(temp_folder + role_files[w])

## Get list of attributes from the CTU file
attributes = list(ctu_pd)

## Get list of role numbers from the role file
role_attribute = roles_pd['Role']
role_count = role_attribute.value_counts()
role_numbers = role_count.index.tolist()

## Create a text file for each attribute
i = 0
for x in attributes:
    if(attributes[i] != 'SrcAddr' and attributes[i] != 'DstAddr' and attributes[i] != 'sTos' and attributes[i] != 'dTos' and attributes[i] != 'State'):
	    text_file = open(temp_folder + attributes[i] + '_' + re.search('\d+', role_files[w]).group() + '.txt', 'w')
	    text_file.close()
    i = i + 1

## This section saves time by creating a combined file for a dataset and role information pair
## Once a combined file has been created it does not need to be created again
## This dramatically reduces compute time for pairs that have already been run at least once

## Check to see if a combined file exists
## If not, then create one by combining the two input files
if not os.path.exists(temp_folder + 'merged_dataframe_' + ctu_files[w].split('.', 1)[0] + '.csv'):
	## Create empty columns for our roles
	ctu_pd['SrcRole'] = ''
	ctu_pd['DstRole'] = ''
	i = 0
	number_of_rows = len(ctu_pd.index)
	while i < number_of_rows:
		## Assign the corresponding role numbers for SrcAddr & DstAddr to SrcRole & DstRole
		SrcRole = roles_pd.loc[roles_pd['Node'] == ctu_pd['SrcAddr'].iloc[i]]
		SrcRole = SrcRole['Role'].iloc[0]
		ctu_pd['SrcRole'].iloc[i] = SrcRole
		DstRole = roles_pd.loc[roles_pd['Node'] == ctu_pd['DstAddr'].iloc[i]]
		DstRole = DstRole['Role'].iloc[0]
		ctu_pd['DstRole'].iloc[i] = DstRole
		## Display a counter on the screen to indicate program is running
		print i
		i = i + 1
	print 'Combined file created'
	## Output the combined dataframe to a .csv
	ctu_pd.to_csv('merged_dataframe_' + ctu_files[w])

## Open the csv for next part (creating histograms)
merged_df = pd.read_csv(temp_folder + 'merged_dataframe_' + ctu_files[w].split('.', 1)[0] + '.csv')

i = 0
j = 0
k = 0
for x in role_numbers:
	for y in role_numbers:
		## Only look at i -> j 
		data_slice = merged_df.loc[merged_df['SrcRole'] == role_numbers[i]]
		data_slice = data_slice.loc[data_slice['DstRole'] == role_numbers[j]]
		
		## Create and process histograms
		for z in attributes:
			attribute = attributes[k]
			
			## Each unique value and the number of times it appears
			counts = data_slice[attribute].value_counts()
			## Number of unique values
			number_unique = len(counts.index)
			## Go to next attribute if the histogram would be empty
			if (number_unique == 0):
				continue
			
			## Process data differently depending on which attribute it is
			if(attribute == 'StartTime'):
				## Convert to date-time, and then to seconds since Unix epoch
				data = pd.to_datetime(data_slice[attribute], format="%Y/%m/%d %H:%M:%S.%f")
				data = pd.DatetimeIndex(data)
				data = data.astype(np.int64) // 10**9
				data = data.astype(float)
			elif(attribute == 'Sport' or attribute == 'Dport'):
				## Force hex conversion 
				data = data_slice[attribute].convert_objects(convert_numeric='force')
			elif(attribute == 'Dur' or attribute == 'TotPkts' or attribute == 'TotBytes' or attribute == 'SrcBytes'):
				data = data_slice[attribute].astype(float)
			elif(attribute == 'Dir' or attribute == 'Proto' or attribute == 'Label'):
				data = data_slice[attribute]
			else:
				## Ignore the unwanted attribute and continue the loop
				k = k + 1
				continue
				   
			## Put it into buckets if there are more than x unique values
			x = 100 
			if number_unique > x:            
				## Put data into buckets
				bucket_size = 100
				while True:
					if(attribute == 'Label'):
						break
					try:
						data = pd.qcut(data, bucket_size).value_counts()
					except ValueError:
						## Decrease bucket size and try again until it fits
						bucket_size = bucket_size - 1
						continue
					break
			
				## Calculate distribution probabilities for bucketed items
				probabilities = []
				for item in data:
					probabilities.append((float(item) / np.sum(data)))
											
				## Append histogram to .txt file
				text_file = open(temp_folder + attribute + '_' + ctu_files[w].split('.', 1)[0] + '.txt', 'a')
				text_file.write(str(role_numbers[i]))
				text_file.write('->')
				text_file.write(str(role_numbers[j]))
				text_file.write(',') 
				text_file.write(str(len(data.index)))
				text_file.write(',')
				labels = data.index.tolist()
				text_file.write(str(labels))
				text_file.write(',') 
				text_file.write(str(probabilities))
				text_file.write('\n')
				text_file.close()
				
			else:
				## Calculate distribution probabilities for non-bucketed items
				probabilities = []
				for item in counts:
					probabilities.append((float(item) / len(data_slice.index)))
				
				## Append histogram to .txt file
				text_file = open(temp_folder + attribute + '_' + ctu_files[w].split('.', 1)[0] + '.txt', 'a')
				text_file.write(str(role_numbers[i]))
				text_file.write('->')
				text_file.write(str(role_numbers[j]))
				text_file.write(',') 
				text_file.write(str(number_unique))
				text_file.write(',')
				text_file.write(str(counts.index.tolist()))
				text_file.write(',') 
				text_file.write(str(probabilities))
				text_file.write('\n')
				text_file.close()
			
			k = k + 1
		k = 0
		j = j + 1
	j = 0
	i = i + 1
i = 0
