import sys
from subprocess import call

inputfile = sys.argv[1]
#inputfile = "CTU13_5_Sample.csv"

print "Starting feature calculation."
call(['python', 'GraphProperties.py', inputfile, 'graph_features.csv'])

print "Finished calculating graph properties."
call(['python', 'role_mining.py', 'graph_features.csv', 'role_information.csv'])

print "Finished role-mining."
call(['python', 'Parameter_Estimation.py', inputfile, 'role_information.csv', 'Param_Roles_Information.csv','node_degree_histogram2.txt'])

print "Finished parameter estimation."
call(['python', 'create_2D_hist_edge_attributes.py', inputfile])

print "Finished calculating edge histogram."
call(['python', 'Simulation_code-version6.py', inputfile,'Param_Roles_Information.csv', 'node_degree_histogram2.txt'])

print "Finished simulating graph."
