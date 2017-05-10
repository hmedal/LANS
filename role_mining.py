import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import scale
from sklearn.neighbors import kneighbors_graph
from sklearn import cluster
import sys

fname = sys.argv[1]
out_file = sys.argv[2]

#fname = "OutputGraphProperties.csv" #"CTU13_11_Features.csv"
#out_file = "Roles_information.csv"

input_data = pd.read_csv(fname,delimiter=',',usecols=[0,1,2,3,4,5,6,7])
features = input_data[[1,2,3,4,5,6,7]].as_matrix()
data = scale(features)

n_samples, n_features = data.shape
n_clusters = 8

minBatchKmeans = cluster.MiniBatchKMeans(n_clusters=n_clusters)
minBatchKmeans.fit(data)

#random_kmeans = KMeans(init='random', n_clusters=n_clusters, n_init=10)
#random_kmeans.fit(data)

#birch = cluster.Birch(n_clusters=n_clusters)
#birch.fit(data)

if hasattr(minBatchKmeans, 'labels_'):
    y_pred = minBatchKmeans.labels_.astype(np.int)
else:
    y_pred = minBatchKmeans.predict(data)

merge_data = pd.DataFrame({'Node': input_data['vertexID'], 'Role' : y_pred})
merge_data.to_csv(out_file, sep=',', columns=['Node','Role'],index=False)
