import pandas as pd
import numpy as np
import sys
import warnings
warnings.simplefilter('ignore', category=FutureWarning)

# File names (Replace with system variables when ready)
#filename = 'CTU13-7.csv'
filename = sys.argv[1]

data = pd.read_csv(filename)
#Get column names
column = list(data)
#Initialize variables
i = 0

data1 = pd.read_csv(filename)
for i in range(0,len(column)):
        #Read in the specified column into pandas
        data = data1[column[i]]
        #Get a count of each item that appears and the number of unique values
        counts = data.value_counts()
        unique_values = len(counts.index)
        
        #Process into buckets if more than 100 unique values
        if unique_values > 100:
            if column[i] == 'SrcAddr' or column[i] == 'DstAddr' or column[i] == 'State' or column[i].lower() == 'label':
                    #Ignore these columns and continue loop
                    i = i + 1
                    continue
            elif column[i] == 'Sport' or column[i] == 'Dport':
                    #Force hex conversion 
                    data = data.convert_objects(convert_numeric='force')
            elif column[i] == 'StartTime':
                    #Convert to date-time, and then to Unix epoch time
                    data = pd.to_datetime(data, format="%Y/%m/%d %H:%M:%S.%f")
                    data = pd.DatetimeIndex(data)
                    data = data.astype(np.int64) // 10**9
            elif column[i] == 'Dur' or column[i] == 'sTos' or column[i] == 'dTos' or column[i] == 'TotPkts' or column[i] == 'TotBytes' or column[i] == 'SrcBytes':
                    #Convert to float
                    data = data.astype(float)
        
            #Put data into buckets
            bucket_size = 100
            while True:
                try:
                    data = pd.qcut(data, bucket_size).value_counts()
                except ValueError:
                        #Decrease bucket size until it fits
                        bucket_size = bucket_size - 1
                        continue
                except Exception as e:
                    print(e)
                break
        
            #Ready the text file for output
            text_file = open(column[i] + ".txt", "w")
        
            #Get the number of buckets and output to file
            number_of_bars = len(data.index)
            text_file.write(str(number_of_bars))
            text_file.write("\n")
        
            #Get the column headers and output to file
            labels = data.index.tolist()
            text_file.write(str(labels))
            text_file.write("\n")
        
            #Get the frequency of each bucket and output to file
            probabilities = []
            for item in data:
                probabilities.append((float(item) / np.sum(data)))
            text_file.write(str(probabilities))
        
            #Close reader
            text_file.close()
            
            i = i + 1
            #data = []
            
        else:
            #Ready the text file for output
            text_file = open(column[i] + ".txt", "w")
        
            #Get the number of unique entries and output to file
            number_of_bars = len(counts.index)
            text_file.write(str(number_of_bars))
            text_file.write("\n")
        
            #Get the column headers and output to file
            labels = counts.index.tolist()
            text_file.write(str(labels))
            text_file.write("\n")
        
            #Calculate the frequency each string appears and output to file
            probabilities = []
            for item in counts:
                probabilities.append((float(item) / len(data.index)))
            text_file.write(str(probabilities))
        
            #Close reader
            text_file.close()
            
            i = i + 1
