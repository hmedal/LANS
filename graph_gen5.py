import random
import time
import ast
import mpi4py
from mpi4py import MPI
import os
import sys
import gc
import numpy


def get_histograms(scenario,input_folder, at_list):
    
    hist_list = dict()
    hist_list['at_list'] = at_list
    for obj in at_list:
        fname = input_folder + obj+"_" +scenario+".txt"
        hists = open(fname,'r')
        contents = dict()
        count = 0
        for each in hists:
            count += 1
            each = each.strip('\n')
            each = each.split('->',1)
            try:
                contents[each[0]]
            except KeyError, e:
                contents[each[0]] = {}
            str = each[1].split(",",2)
            if str[2] != '[],[]':
                contents[each[0]][str[0]] = str[2]
        hists.close()
        hist_list[obj] = contents

    return hist_list


def to_edge(temp_folder, source, dest):
    fsource = open(temp_folder +'samples.csv','r')
    tempar = []
    for each in fsource:
        tempar.append(each)
    ran = random.randint(0,len(tempar)-1)
    line = tempar[ran].strip('\n').split(',')
    line[0] = str(source)
    line[1] = str(dest)
    st = ''
    for each in line:
        st += each + ','
    line = st.strip(',')
    return line


def get_size(size_input,temp_folder):
    size_file = open(temp_folder +'Param_Roles_Information' + size_input.split('.')[0] + '.csv','r')
    size_ar = []
    for each in size_file:
        size_ar.append(each)
    size_file.close()
    size_ar[0] = int(float(size_ar[0].strip(' ').strip('\n')))
    loc = size_ar[0] * size_ar[0] + size_ar[0] * 2 + size_ar[0] + 4
    count = 0
    accum = 0
    while count < loc + size_ar[0]:
        if count >= loc:
            accum += int(float(size_ar[count]))
        count += 1
    return accum


def setstart(r1,TM):
    flag = -1
    for each in TM[r1][r1]:
        if each > 0.01:
            flag = r1
    count = 0
    if flag == -1:
        for each in TM[r1]:
            for item in each:
                if item > 0.01:
                    flag = count
            count += 1
    if flag == -1:
        flag = 0
    return flag


def set_attr(temparray):
    rng = random.randint(1,len(temparray)-1)
    res = []
    count = 0
    for each in temparray[rng]:
        if count != 0 and count != 1 and count != 2:
            each = each.strip('\n')
            res.append(each)
        count += 1
    return [res]


def attributes(attribute_histograms,srole,drole):
    at_list = attribute_histograms['at_list']
    line = ""
    warn = False
    for attr in at_list:
        rand = random.randint(0,1000000)
        rand = float(rand)/1000000
        accumulator = float(0)
        count = 0
        if len(attribute_histograms[attr]) != 0:
            try:
                val = attribute_histograms[attr][str(srole)][str(drole)]
                val = ast.literal_eval(val)
            except KeyError, e:
                choice = random.choice(attribute_histograms[attr].keys())
                chosen = random.choice(attribute_histograms[attr][choice].keys())
                val = attribute_histograms[attr][choice][chosen]
                warn = True
                val = ast.literal_eval(val)
            for each in val[1]:
                try:
                    accumulator += float(each)
                except Exception,e:
                    print val
                    print each
                    print "FAILURE: Histogram error"
                    sys.exit(1)
                if accumulator < rand:
                    count += 1
            if count >= len(val[0]):
                count = len(val[0]) - 1
            val = val[0][count]
            val = str(val)
            x = val
            val = val.strip("(")
            val = val.strip("]")
            if x != val:
                try:
                    val = ast.literal_eval(val)
                except SyntaxError,e:
                    val = val.strip('(')
                    val = val.strip('[')
                    val = val.strip(')')
                    val = val.strip(']')
                    val = val.split(',')
                    val = tuple(val)
            if type(val) is tuple:
                rand = random.uniform(float(val[0]),float(val[1]))
                if "ur" in attr or "ime" in attr:
                    val = rand
                else:
                    val = int(rand)


        else:
            val = 0
        line = line + str(val) + ","
    return line,warn


def generate_node(nodes,r,degree_array,len_deg_ar,TM):
    l = 0
    for each in nodes:
        l += len(each)
    RN = float(random.randint(0,999999999999))
    RN = RN/1000000000000
    count = 0
    i = 0
    flag1 = 0
    startrole = r
    for each in TM[r][r]:
        if each != 0:
            flag1 = 1
    if flag1 == 0:
        for each in TM[r]:
            if i == -1:
                for item in each:
                    if item != 0:
                        i = 0
                        startrole = count
            count += 1
    count = 0
    i = 0
    if len(degree_array) <= r:
        return [l + 1,startrole,1,1]
    elif len(degree_array[r]) == 0:
        return [l + 1,startrole,1,1]
    for each in degree_array[r]:
        count += each[2]
        if count >= RN:
            break
        else:
            i += 1
    indeg = [degree_array[r][i][0]]
    outdeg = [degree_array[r][i][1]]
    if (i+1) % len_deg_ar == 0:
        outdeg.append(degree_array[r][i][1])
    elif i % len_deg_ar == 0:
        outdeg.append(degree_array[r][i][1])
    else:
        outdeg.append(degree_array[r][i+1][1])
    if i + len_deg_ar >= len(degree_array):
        indeg.append(degree_array[r][i][0])
    else:
        indeg.append(degree_array[r][i+len_deg_ar][0])
    inrange = indeg
    outrange = outdeg

    #indeg.append(degree_array[r][len(degree_array[r])-1][0]/len_deg_ar + indeg[0])
    #outdeg.append(degree_array[r][len(degree_array[r])-1][1]/len_deg_ar + outdeg[0])
    if int(indeg[0]) == int(indeg[1]):
        indeg = int(indeg[0])
    elif(int(indeg[0] + 1) == int(indeg[1])):
        indeg = random.randint(int(indeg[0]),int(indeg[1]))
    else:
        if int(indeg[1]) - int(indeg[0]) > 1:
            indeg[1] -= 1
        try:
            indeg = random.randint(int(indeg[0]),int(indeg[1]))
        except Exception:
            print "failure generating node:"
            print "indegree range :",indeg
            print "value inside histogram:",i
            print "Buckets Per histogram subsection:",len_deg_ar
            print "bucket base:", degree_array[r][i]
            print "bucket max:",degree_array[r][i+1][1]

            sys.exit(1)
    if int(outdeg[0]) == int(outdeg[1]):
        outdeg = int(outdeg[0])
    else:
        if int(outdeg[1]) - int(outdeg[0]) > 1:
            outdeg[1] -= 1
        try:
            outdeg = random.randint(int(outdeg[0]),int(outdeg[1]))
        except Exception:
            print "failure generating node:"
            print "outdegree range :",outdeg
            print "value inside histogram:",i
            print "Buckets Per histogram subsection:",len_deg_ar
            print "bucket base:", degree_array[r][i]
            print "bucket max:",degree_array[r][i+1][1]

            sys.exit(1)
    if indeg == 0 and outdeg == 0:
        outdeg = 1

    if int(inrange[0]) == int(inrange[1]) and inrange[1] > int(inrange[1]):
        print "case:1"
    if int(outrange[0]) == int(outrange[1]) and outrange[1] > int(outrange[1]):
        print "case:2"

    return [l + 1,startrole,indeg,outdeg]

def generate_node_exact(nodes,r,degree_array,len_deg_ar,TM,degpos):
    l = 0
    for each in nodes:
        l += len(each)
    count = 0
    i = 0
    flag1 = 0
    startrole = r
    for each in TM[r][r]:
        if each != 0:
            flag1 = 1
    if flag1 == 0:
        for each in TM[r]:
            if i == -1:
                for item in each:
                    if item != 0:
                        i = 0
                        startrole = count
            count += 1
    count = 0
    i = 0
    if len(degree_array) <= r:
        return [l + 1,startrole,1,1]
    elif len(degree_array[r]) == 0:
        return [l + 1,startrole,1,1]
    for each in degree_array[r]:
        count += each[2]
        if count >= degpos:
            break
        else:
            i += 1
    indeg = [degree_array[r][i][0]]
    outdeg = [degree_array[r][i][1]]
    if (i+1) % len_deg_ar == 0:
        outdeg.append(degree_array[r][i][1])
    elif i % len_deg_ar == 0:
        outdeg.append(degree_array[r][i][1])
    else:
        outdeg.append(degree_array[r][i+1][1])
    if i + len_deg_ar >= len(degree_array):
        indeg.append(degree_array[r][i][0])
    else:
        indeg.append(degree_array[r][i+len_deg_ar][0])

    #indeg.append(degree_array[r][len(degree_array[r])-1][0]/len_deg_ar + indeg[0])
    #outdeg.append(degree_array[r][len(degree_array[r])-1][1]/len_deg_ar + outdeg[0])
    if int(indeg[0]) == int(indeg[1]):
        indeg = int(indeg[0])
    elif(int(indeg[0] + 1) == int(indeg[1])):
        indeg = random.randint(int(indeg[0]),int(indeg[1]))
    else:
        indeg = random.randint(int(indeg[0]),int(indeg[1] - 1 ))
    if int(outdeg[0]) == int(outdeg[1]):
        outdeg = int(outdeg[0])
    else:
        if int(outdeg[1]) - int(outdeg[0]) > 1:
            outdeg[1] -= 1
        try:
            outdeg = random.randint(int(outdeg[0]),int(outdeg[1]))
        except Exception:
            print "failure generating node:"
            print "outdegree range :",outdeg
            print "value inside histogram:",i
            print "Buckets Per histogram subsection:",len_deg_ar
            print "bucket base:", degree_array[r][i]
            print "bucket max:",degree_array[r][i+1][1]

            sys.exit(1)
    if indeg == 0 and outdeg == 0:
        outdeg = 1

    return [l + 1,startrole,indeg,outdeg]




def generate_edge2(f,TM,nodes,innodes,outnodes):
    #determines the start and end point for an edge to be added to the graph, also modifies innodes and outnodes
    if len(outnodes[f]) > 1:
        gen = random.randint(0, len(outnodes[f])-1)
    elif len(outnodes[f]) == 1:
        gen = 0
    else:
        count = 0
        f = -1
        for each in outnodes:
            if len(each) >= 1:
                f = count
            count += 1
        if f >= 0:
            if len(outnodes[f]) > 1:
                gen = random.randint(0, len(outnodes[f])-1)
            else:
                gen = 0
        else:
            return 'complete', 'complete'
    p = outnodes[f][gen][1]  # previous role connection
    i = outnodes[f][gen][0]  # node ID
#    count = 0
#    for each in innodes:
#        for item in each:
#            if int(item[2]) < 1:
#                innodes[count].remove(item)
#        count += 1
    j = TM[f][p]  # matrix entry for previously connected role
    k = float(random.randint(0, 10000))/10000
    dest = -1
    count = 0
    for each in nodes:  # assign the destination role to a non-empty role
        if len(each) != 0:
            dest = count
            break
        count += 1

    tmp = 0
    count = 0
    for each in j:  # determine, based on probability, which role we should connect to
        tmp += each
        if tmp >= k:
            dest = count
            break
        count += 1
    flag2 = 0
    if len(innodes) > dest:  # make sure we didnt select an invalid role
        if len(innodes[dest]) > 1:  # if the role is has nodes in it, we will choose a destination from it
            tflag = 1
            while tflag == 1:
                k = random.randint(0,len(innodes[dest])-1)
                if int(innodes[dest][k][2]) < 1:
                    innodes[dest].remove(innodes[dest][k])

                else:
                    tflag = 0
        elif len(innodes[dest]) == 1:  # one entry condition
            k = 0
        else:
            flag2 = 1
            k = random.randint(0,len(nodes[dest])-1)  # choosing a random value from the chosen destination role

    if flag2 == 1:

        count = 0
        for each in innodes:
            count += len(each)
        if count >= 1:
            tmp = random.randrange(0,count)
            count = 0
            k = -1
            for each in innodes:
                if len(each) <= tmp:
                    tmp -= len(each)
                    count += 1
                elif k == -1:
                    k = tmp
                    tmp -= len(each)
                    dest = count
        else:
            ret = nodes[dest][k][0]
            outnodes[f][gen][2] = dest
            outnodes[f][gen][3] -= 1  # decrement the outdegree of the source node
            if outnodes[f][gen][3] == 0:  # if the source node is now empty of out degree, we remove it from the list
                outnodes[f].remove(outnodes[f][gen])
            global totalflag
            if totalflag == 1:
                return dest, i, ret,innodes,outnodes,nodes
            else:
                for each in innodes:
                    if len(each) > 0:
                        for item in each:
                            print item
                    else:
                        print each
                #return dest, i, ret,innodes,outnodes,nodes
                print "report this error to Chris", innodes[dest][k][0]
                sys.exit(-1)

    try:
        ret = innodes[dest][k][0]  # get the ID for the chosen destination node     IF IN ERROR, REPORT TO CHRIS
    except Exception, e:
        print len(innodes[dest])
        print dest
        print k
        for each in innodes:
            print len(each)
        sys.exit()
#    acc = 0  # 5 lines of testcode to calculate total remaining indegree
#    for each in innodes:
#        for item in each:
#            acc += int(item[2])
#    nodedata = innodes[dest][k]
    innodes[dest][k][2]  = int(innodes[dest][k][2]) - 1  # decrement the indegree for destination node
    #print innodes[dest][k][2]
    if innodes[dest][k][2] <= 0:  # if after decrement the indegree becomes zero, we remove it from innodes
        innodes[dest].remove(innodes[dest][k])
#    acc2 = 0  # 12 lines of testcode to calculate total remaining indegree, compare it to
#    for each in innodes:
#        for item in each:
#            acc2 += int(item[2])
#    if acc != acc2 + 1:
#        print "invalid node removed"
#        print acc, acc2
#        print nodedata
#        print len(innodes[dest])
#        print dest
#        print len(innodes)
#        sys.exit(0)
    outnodes[f][gen][2] = dest  # Update previously connected role in the source node
    outnodes[f][gen][3] -= 1  # decrement the outdegree of the source node
    if outnodes[f][gen][3] == 0:  # if the source node is now empty of out degree, we remove it from the list
        outnodes[f].remove(outnodes[f][gen])

    return dest, i, ret,innodes,outnodes,nodes


def generate_edge_old(f,TM,nodes):
    if len(outnodes[f]) > 1:
        gen = random.randint(0, len(outnodes[f])-1)
    elif len(outnodes[f]) == 1:
        gen = 0
    else:
        count = 0
        f = -1
        for each in outnodes:
            if len(each) >= 1:
                f = count
            count += 1
        if f >= 0:
            gen = random.randint(0, len(outnodes[f])-1)
        else:
            return 'complete', 'complete'
    p = outnodes[f][gen][1]
    i = outnodes[f][gen][0]

    j = TM[f][p]
    k = float(random.randint(0, 10000))/10000
    dest = -1
    count = 0
    for each in nodes:
        if len(each) != 0:
            dest = count
            break
        count += 1
    tmp = 0
    count = 0
    for each in j:
        tmp += each
        if tmp >= k:
            dest = count
            break
        count += 1
    flag2 = 0

    if len(innodes) > dest:
        if len(innodes[dest]) > 1:
            k = random.randint(0,len(innodes[dest])-1)
        elif len(innodes[dest]) == 1:
            k = 0
        else:
            flag2 = 1
            k = random.randint(0,len(nodes[dest])-1)

    if flag2 == 0:
        ret = innodes[dest][k][0]
        innodes[dest][k][2] -= 1
        if innodes[dest][k][2] == 0:
            innodes[dest].remove(innodes[dest][k])
    else:
        ret = nodes[dest][k][0]

    outnodes[f][gen][2] = dest
    outnodes[f][gen][3] -= 1
    if outnodes[f][gen][3] == 0:
        outnodes[f].remove(outnodes[f][gen])

    return dest, i, ret


def add_edge(b,c,attribute_histograms,srole,drole,mal_role):

    d,warn = attributes(attribute_histograms,srole,drole)
    d = d.split(",")
    ret = [b,c]
    for each in d:
        ret.append(each)
    if('otnet' in ret[-1] or 'alicious' in ret[-1]):
        ret[-1] = 'flow = background'
    if(srole == mal_role):
        ret[-1] = 'flow = Botnet Activity'
    return ret,warn


def setup_and_files(temp_folder,scenario,seed):
    fname = scenario.split('.')
    infilename = "_" + scenario
    scenario = ''
    count = 1
    if len(fname) == 1:
        scenario = fname[0]
    for each in fname:
        if count < len(fname):
            scenario = scenario + each
        count += 1
    if(seed == 0):
        seed = time
    random.seed(seed)
    roles = 0
    input_folder = os.path.dirname(os.path.dirname(temp_folder))
    #input_folder += "/input_files/"
    input_folder += "\\input_files\\"
    at_list = ["Proto","StartTime","Dur","Sport","Dir","Dport","State","sTos","dTos","TotPkts","TotBytes","Label"]
    attribute_histograms = get_histograms(scenario,temp_folder, at_list)
    #print attribute_histograms['at_list']
    #sys.exit(0)
    mal_role = temp_folder+ "malicious_role" +scenario + ".txt"
    mal_role = open(mal_role,'r')
    for each in mal_role:
        mal_role = int(each)
        break
    if len(fname) > 1:
        name_inputfile = input_folder+ scenario + "." + fname[-1]#name_inputfile = "" + scenario + "." + fname[-1]
    else:
        name_inputfile = input_folder+ scenario

    #print attribute_histograms['attributes']
    #sys.exit(0)

    TPM = open(temp_folder+"Param_Roles_Information" + scenario + ".csv", 'r')
    histfile = open(temp_folder+"node_degree_histogram2"+ scenario + ".txt", 'r')
    degree_array = []
    for each in histfile:
        degree_array.append(each)
    count = 0
    iterator = 0
    for each in degree_array:
        if count > 0:
            degree_array[count] = each.strip('\n').strip(' ').split(',')
            for item in degree_array[count]:
                degree_array[count][iterator] = float(item)
                iterator += 1
            iterator = 0
        count += 1
    histfile.close()
    return at_list,attribute_histograms,mal_role,TPM,degree_array


def setupAndFiles(temp_folder,scenario,seed):
    fname = scenario.split('.')
    infilename = "_" + scenario
    scenario = ''
    count = 1
    if len(fname) == 1:
        scenario = fname[0]
    for each in fname:
        if count < len(fname):
            scenario = scenario + each
        count += 1
    if(seed == 0):
        seed = time
    random.seed(seed)
    roles = 0
    input_folder = os.path.dirname(os.path.dirname(temp_folder))
    #input_folder += "/input_files/"
    input_folder += "\\input_files\\"
    at_list = ["Proto","StartTime","Dur","Sport","Dir","Dport","State","sTos","dTos","TotPkts","TotBytes","Label"]
    attribute_histograms = get_histograms(scenario,temp_folder, at_list)
    #print attribute_histograms['at_list']
    #sys.exit(0)
    mal_role = temp_folder+ "malicious_role" +scenario + ".txt"
    mal_role = open(mal_role,'r')
    for each in mal_role:
        mal_role = int(each)
        break
    if len(fname) > 1:
        name_inputfile = input_folder+ scenario + "." + fname[-1]#name_inputfile = "" + scenario + "." + fname[-1]
    else:
        name_inputfile = input_folder+ scenario


    TPM = open(temp_folder+"Param_Roles_Information" + scenario + ".csv", 'r')
    histfile = temp_folder+"node_degree_histogram2"+ scenario + ".txt"
    histlist = read_node_histograms(histfile)
    return at_list,attribute_histograms,mal_role,TPM,histlist



def setup_and_files_cont(degree_array,TPM,deg_flag):
    count = 0
    iterator = 0
    TA = []
    for each in degree_array:
        if count > 0:
            if count == 1:
                TA.append([])
            TA[iterator].append(each)
        if count >= int(degree_array[0])*int(degree_array[0]):
            count = 0
            iterator += 1
        count += 1
    lendegar = int(degree_array[0])
    degree_array = TA
    count = float(0.0)
    iterator = 0
    for each in degree_array:
        for item in each:
            count += item[2]
        degree_array[iterator].append([count,count,count,1,1,1])
        count = 0
        iterator += 1
    count = 0
    iterator = 0
    if deg_flag == 0:
        for each in degree_array:
            for item in each:
                if degree_array[count][len(degree_array[count]) - 1][0] != 0:
                    degree_array[count][iterator][2] = item[2] / degree_array[count][len(degree_array[count]) - 1][0]
                    iterator += 1
            count += 1
            iterator = 0
    count = 0
    len(degree_array)
    for each in degree_array:
        degree_array[count].remove(degree_array[count][len(degree_array[count])-1])
        count += 1
    TM = []
    GI = []
    count = -1
    i = 0
    for each in TPM:
        if count == -1:
            roles = int(float(each.strip(',').strip('\n')))
        if count > 0:
            GI.append(each.strip('\n'))
        count += 1
        if count > roles:
            if i < roles:
                TM.append(GI)
                GI = []
            count = 0
            i += 1
    TPM.close()
    tp = []
    for each in GI:
        x = each.strip(',')
        if len(x) > 0:
            tp.append(x)
    tmp = []
    tp = []
    a = []
    for each in TM:
        for item in each:
            item = item.split(',')
            for part in item:
                part = float(part)
                tp.append(part)
            tmp.append(tp)
            tp = []
        a.append(tmp)
        tmp = []

    TM = a
    count = 0
    a = []
    while count < roles:
        a.append(0)
        count += 1
    count = 0
    while count < roles:
        x = GI[(len(GI)-1-roles+count)]
        a[count] = int(float(x))
        count += 1
    return roles,TM,degree_array,a,lendegar,GI


def setupAndFilesCont(TPM):
    TM = []
    GI = []
    count = -1
    i = 0
    for each in TPM:
        if count == -1:
            roles = int(float(each.strip(',').strip('\n')))
        if count > 0:
            GI.append(each.strip('\n'))
        count += 1
        if count > roles:
            if i < roles:
                TM.append(GI)
                GI = []
            count = 0
            i += 1
    TPM.close()
    tp = []
    for each in GI:
        x = each.strip(',')
        if len(x) > 0:
            tp.append(x)
    tmp = []
    tp = []
    a = []
    for each in TM:
        for item in each:
            item = item.split(',')
            for part in item:
                part = float(part)
                tp.append(part)
            tmp.append(tp)
            tp = []
        a.append(tmp)
        tmp = []

    TM = a
    count = 0
    a = []
    while count < roles:
        a.append(0)
        count += 1
    count = 0
    while count < roles:
        x = GI[(len(GI)-1-roles+count)]
        a[count] = int(float(x))
        count += 1
    return roles,TM,a,GI



def node_creation(roles,TM,degree_array,lendegar,a,startpoint,GI,deg_flag = 0):

    # this function creates the list of nodes (vertices) for the graph including the in and out degree and assigned role
    count = 0
    role = 0
    nodes = []
    startpos = []
    gc.collect()
    # generate a list of lists to contain the nodes
	# each role gets it's own list of member nodes
    while count < roles:
        startpos.append(setstart(count,TM))
        count += 1
    count = 0
    #generate the nodes to fill the required number for each role
    if deg_flag == 0:
        while role < roles:
            nodes.append([])
            while count < a[role]:
                nodes[role].append(generate_node(nodes,role,degree_array,lendegar,TM))
                count += 1
                if count % 10000 == 0:
                    print count
            role += 1
            count = 0
    else:
        while role < roles:
            nodes.append([])
            while count < a[role]:
                nodes[role].append(generate_node_exact(nodes,role,degree_array,lendegar,TM,count))
                count += 1
                if count % 10000 == 0:
                    print count
            role += 1
            count = 0
		
    degree_array = []
    count = 1
    total = 0
    indegreecounter = 0
    outdegreecounter = 0
    fileout1 = open("C:\\Users\\Chris\\Desktop\\GraphSimulation5\\degreefile.txt",'w')
    fileout1.write("NODE ID, ROLE#,INDEGREE,OUTDEGREE\n")
    for each in nodes:
        for item in each:
            for val in item:
                fileout1.write(str(val)+",")
            fileout1.write("\n")
            indegreecounter += 1
            outdegreecounter += 1
            item[0] = count
            count += 1
    fileout1.close()
    
    count = 0
    for each in nodes:
        for item in each:
            count += 1
            item[0] += startpoint
    count = 0
    RPM = []
    while count < roles:
        n = GI[count]
        n = n.split(',')
        o = []
        for m in n:
            o.append(float(m) / float(GI[roles*3]))
        count += 1
        RPM.append(o)

    # these loops create the indegree and outdegree objects that are used to determine where edges when they are generated
	# they also delete any entries that have a zero for the associated degree so that node will not be used in the associated direction
    global innodes
    global outnodes
    for each in nodes:
        outnodes.append(each[:])
        innodes.append(each[:])
    count = 0
    iterator = 0
    for each in innodes:
        if len(each) != 0:
            for item in each:
                if int(item[2]) <= 0:
                    print innodes[count][iterator]
                    innodes[count].remove(item)
                    print innodes[count][iterator]
                    sys.exit(0)
                iterator += 1

        count += 1
    for each in innodes:
        for item in each:
            print item
    sys.exit(1)
    count = 0
    for each in nodes:
        if len(each) != 0:
            for item in each:
                if int(item[3]) <= 0:
                    outnodes[count].remove(item)
        count += 1

    return nodes,innodes,outnodes,RPM


def read_node_histograms(loc):
    f1 = open(loc,"r")
    i = 0
    num = -1
    histlist = []
    l1 = []
    for each in f1:
        each = each.strip("\n")
        if i < num:
            each = each.split(",")
            obj = each[4].strip("\n")
            obj = float(obj)
            if obj >= 1.0:
                l1.append(each)
            i += 1
        elif num == -1:
            num = int(each) * int(each)
        else:
            i = 0
            num = int(each) * int(each)
            histlist.append(l1)
            l1 = []
    if l1 != []:
        histlist.append(l1)
    return histlist


def make_node(num,bucket,rolecounter):
    node = []
    indeg = numpy.random.randint(int(float(bucket[0])),int(float(bucket[1])))
    outdeg = numpy.random.randint(int(float(bucket[2])),int(float(bucket[3])))
    node.append(num)
    node.append(rolecounter)
    node.append(indeg)
    node.append(outdeg)

    return node


def node_generator(nodecounter,bucket,rolecounter):
    generated = []
    num = int(float(bucket[4]))
    i = 0
    while i < num:
        val = nodecounter + i
        generated.append(make_node(val,bucket,rolecounter))
        i += 1
    return generated


def node_maker(histograms):
    nodecounter = 0
    rolecounter = 0
    l1 = []
    for item in histograms:
        elist = []
        l1.append(elist)
        for each in item:
            num = each[4]
            if int(float(num)) > 0:
                temp = node_generator(nodecounter,each,rolecounter)
                for each in temp:
                    l1[rolecounter].append(each)
            nodecounter += int(float(num))

        rolecounter += 1
    return l1


def nodeCreation(roles,TM,a,startpoint,GI,histlist,innodes,outnodes):

    # this function creates the list of nodes (vertices) for the graph including the in and out degree and assigned role
    count = 0
    startpos = []
    gc.collect()
    #generate the nodes
    nodes = node_maker(histlist)
    # each role gets it's own list of member nodes
    while count < roles:
        startpos.append(setstart(count,TM))
        count += 1
    #generate the nodes to fill the required number for each role
    count = 1
    total = 0
    indegreecounter = 0
    outdegreecounter = 0
    fileout1 = open("C:\\Users\\Chris\\Desktop\\GraphSimulation5\\degreefile.txt",'w')
    fileout1.write("NODE ID, ROLE#,INDEGREE,OUTDEGREE\n")
    for each in nodes:
        for item in each:
            for val in item:
                fileout1.write(str(val)+",")
            fileout1.write("\n")
            indegreecounter += 1
            outdegreecounter += 1
            item[0] = count
            count += 1
    fileout1.close()

    count = 0
    for each in nodes:
        for item in each:
            count += 1
            item[0] += startpoint
    count = 0
    RPM = []
    while count < roles:
        n = GI[count]
        n = n.split(',')
        o = []
        for m in n:
            o.append(float(m) / float(GI[roles*3]))
        count += 1
        RPM.append(o)

    count = 0
    iterator = 0

    # these loops create the indegree and outdegree objects that are used to determine where edges when they are generated
	# they also delete any entries that have a zero for the associated degree so that node will not be used in the associated direction

    for each in nodes:
        outnodes.append([])
        innodes.append([])
        for item in each:
            outnodes[count].append([])
            innodes[count].append([])
            for obj in item:
                outnodes[count][iterator].append(obj)
                innodes[count][iterator].append(obj)
            iterator += 1
        iterator = 0
        count += 1
    count = 0
    iterator = 0
    for each in nodes:
        if len(each) != 0:
            for item in each:
                if int(item[2]) <= 0:
                    innodes[count].remove(item)
                iterator += 1


        count += 1


    count = 0
    for each in nodes:
        if len(each) != 0:
            for item in each:
                if int(item[3]) == 0:
                    outnodes[count].remove(item)
        count += 1
    intotal = 0
    intop = 0
    for each in innodes:
        for item in each:
            intotal += item[2]
            if item[2] > intop:
                intop = item[2]
    outtotal = 0
    for each in outnodes:
        for item in each:
            outtotal += item[3]
    global totalflag
    totalflag = 0
    if intotal <= outtotal:
        totalflag = 1

    return nodes,RPM,innodes,outnodes


def edge_creation(RPM,TM,nodes,attribute_histograms,mal_role,innodes,outnodes):

    # this function generates the edgelist that will become the final graph when written to file
    edgelist = []
    count = 0
    flag = 0
    warn = False
    warn_flag = False
    # this loop continues until the total remaining outdegree is equal to zero
    while flag != 1:
        role = 0
        x = float(random.randint(0,99999999))
        x /= 100000000
        accumulator = 0
        for each in RPM:
            for item in each:
                if accumulator <= x:
                    accumulator += item
            if accumulator <= x:
                role += 1
        x,y,z,innodes,outnodes,nodes = generate_edge2(role,TM,nodes,innodes,outnodes)
        if y == 'complete':
            break
        if y != z:
            edge,warn = add_edge(y,z,attribute_histograms,role,x,mal_role)
            edgelist.append(edge)
        flag = 1
        if warn == True:
            # this is set for indication of whether the warning file needs to contain a warning or an all clear message
            warn_flag = True
        for each in outnodes:
            if len(each) > 0:
                flag = 0
        if count % 10000 == 0 and count > 0:
            print count
        count += 1
    return edgelist,warn_flag,innodes,outnodes


def write_graph_to_file(temp_folder,at_list,edgelist):
    # this function creates the graph file and 
    comm = mpi4py.MPI.COMM_WORLD
    MPI_rank = comm.Get_rank()
    #name_outputfile = "SimulatedGraph/localgen_" + str(MPI_rank)+'.csv'
    name_outputfile = os.path.dirname(os.path.dirname(temp_folder))
    #name_outputfile = name_outputfile + "/SimulatedGraph/localgen_" + str(MPI_rank)+'.csv'
    name_outputfile = name_outputfile + "\\SimulatedGraph\\localgen_" + str(MPI_rank)+'.csv'
	
    # starts the graph file with a header for identification of collumns 
    outfile = open(name_outputfile, 'wb')
    topline = "source,destination,"
    for each in at_list:
        topline = topline + each + ","
    outfile.write((topline + '\n').encode("utf-8"))
    count = 0
    miscfile = open(temp_folder+'samples.csv','wb')
    i = 0
    #this series of loops writes the graph to a file and also creates a samples file for enterprise connection
    for each in edgelist:
        for item in each:
            count += 1
            outfile.write(str(item).encode("utf-8"))
            if(i < 50 and each[-1] != 'flow=Botnet Activity' and len(each) > 2 and MPI_rank == 0):
                miscfile.write(str(item).encode("utf-8"))
                if count < len(each):
                    miscfile.write(','.encode("utf-8"))
                else:
                    miscfile.write('\n'.encode("utf-8"))
                    i += 1
            if count < len(each):
                outfile.write(','.encode("utf-8"))
            else:
                outfile.write('\n'.encode("utf-8"))
                count = 0


def write_warning_file(temp_folder,warn_flag):

    # writes a warning file that indicates whether indgree ran out or if there has been an error with histograms
    LOC = os.path.dirname(os.path.dirname(temp_folder))
    #warn = open(LOC+'/Warning_File.txt',"wb")
    warn = open(LOC+'\\SimulatedGraph\\Warning_File.txt',"w")
    if warn_flag == True:
        warn.write("WARNING\n")
        warn.write("detected edge connection between unanticipated roles or missing histogram\n")
        warn.write("edge attributes have been randomly assigned from existing histograms for any such connections\n")
    else:
        warn.write("generation complete with no warnings")


def create_graph(temp_folder,scenario,seed = 0,startpoint = 0):
    innodes = []
    outnodes = []
    deg_flag = 1 # set this to opt for exact values from degree histogram rather than probabiltiy
    # this is the primary controller function 
    at_list,attribute_histograms,mal_role,TPM,histlist = setupAndFiles(temp_folder,scenario,seed)
    print "early setup"
    roles,TM,a,GI = setupAndFilesCont(TPM)
    print "late setup"
    nodes,RPM,innodes,outnodes = nodeCreation(roles,TM,a,startpoint,GI,histlist,innodes,outnodes)
    print "nodes generated"
    edgelist,warn_flag,innodes,outnodes = edge_creation(RPM,TM,nodes,attribute_histograms,mal_role,innodes,outnodes)
    print "edges generated"
    write_graph_to_file(temp_folder,at_list,edgelist)

    acc = 0
    for each in innodes:
        for item in each:
            acc += int(item[2])
    print acc

    write_warning_file(temp_folder,warn_flag)


if __name__ == "__main__":

    start = time.time()
    #create_graph("5",startpoint=0)
    TF = "C:\\Users\\Chris\\Desktop\\role4_bins200\\temp\\"
    #TF = "/work/fz56/Graph-Simulation-4-master/newfolder/"
    create_graph(TF,"11",seed=0)
    print(time.time() - start)
    #comm = mpi4py.MPI.COMM_WORLD
    #MPI_size = comm.Get_size()
    #MPI_rank = comm.Get_rank()