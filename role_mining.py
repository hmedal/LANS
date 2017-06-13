import pandas as pd
import numpy as np
from sklearn.preprocessing import scale
from sklearn import cluster
import os
from os import listdir
import warnings
import Read_Params as rp
warnings.filterwarnings("ignore")


def find_all_filenames(path_to_dir):
    filenames = listdir(path_to_dir)
    return filenames

params = rp.Read_Params().Params
n_clusters = params['nRole']
conf_file = open('Configuration.txt',"r")


lineList = conf_file.readlines()
conf_file.close()
input_folder = lineList[-1]+"/input_files/"

allFiles = find_all_filenames(input_folder)
temp_folder = os.path.dirname(os.path.dirname(input_folder))
temp_folder += "/temp/"

#if not os.path.exists(temp_folder):
    #os.makedirs(temp_folder)

for f in allFiles:
    data_file = input_folder+f
    feature_file = temp_folder+"Properties"+ f.split('.')[0] +".csv"

    feature_data = pd.read_csv(feature_file,delimiter=',',usecols=[0,1,2,3,4,5,6])
    features = feature_data[[1,2,3,4,5,6]].as_matrix()
    data = scale(features)
    n_samples, n_features = data.shape

    predictor = cluster.MiniBatchKMeans(n_clusters=n_clusters)
    predictor.fit(data)

    #predictor = KMeans(init='random', n_clusters=n_clusters, n_init=10)
    #predictor.fit(data)

    #predictor = cluster.Birch(n_clusters=n_clusters)
    #predictor.fit(data)

    if hasattr(predictor, 'labels_'):
        y_pred = predictor.labels_.astype(np.int)
    else:
        y_pred = predictor.predict(data)


    #----------------------------- Malicious Role Classification -------------------------------#
    df = pd.read_csv(data_file, delimiter=',', usecols=[3,14])
    bot_data = df[df['Label'].str.contains("Botnet")]
    botIP_list = bot_data.SrcAddr.unique()
    len_botIPs = len(botIP_list)
    feature_matrix = []

    for i in range(len(botIP_list)):
        bot_index = feature_data[feature_data['IPs'] == botIP_list[i]].index
        bot_feature = data[bot_index, 0:6].reshape((1,n_features))
        feature_matrix.append(bot_feature)
        avg_vals = np.mean(feature_matrix, axis=0)

    #Find closest cluster
    bot_cluster = predictor.predict(avg_vals)


    #----------------------------- Write all data -------------------------------#
    out_file = temp_folder+"role_information"+ f.split('.')[0] +".csv"
    malicious_role_file = temp_folder+"malicious_role"+f.split('.')[0]+".txt"

    malicious_role_file = open(malicious_role_file,'w')
    np.savetxt(malicious_role_file, bot_cluster[np.newaxis], fmt="%d", delimiter=",")

    merge_data = pd.DataFrame({'Node': feature_data['IPs'], 'Role' : y_pred})
    merge_data.to_csv(out_file, sep=',', columns=['Node','Role'],index=False)