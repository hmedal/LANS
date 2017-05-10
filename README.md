# Graph-Simulation-2
This project is to generate simulated graph from a given graph. This script has strong dependency on the name of the columns, types of the columns and format of the input file. This script can only take csv files as input.

To run this script Python 2.7 is required along with the following packages: numpy 1.10.4, panda 0.18.0, random, csv, gc (garbage collector), sklearn 0.18.1(cluster, KMeans, kneighbors_graph, scale), Networkx 1.11, sys, subprocess 

Before running the script, put the code and the input files in the same folder.	

To run the script with the small dataset (5549 nodes and 12980 edges), run the masterScript with the following command:
	
  python masterscript.py "CTU13_5_Sample.csv"

To run the script with the large dataset (CTU-13-5 netflow data, contains 41658 nodes and 129832 edges), run the masterscript with the following command:
	
  python masterscript.py "CTU13_5.csv"
  
  To run the script with a binetflow file for the large dataset (CTU-13-5 netflow data, contains 41658 nodes and 129832 edges), run the masterscript with the following command:
	
  python masterscript.py "5.binetflow"
	
  This script generates a simulated graph named simulated_graph.csv
  
  To run all three of these tests, use the "runTests.sh" script.
