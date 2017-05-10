import random
import time
import gc
import ast
import mpi4py
from mpi4py import MPI
import os
import sys

innodes = []
outnodes = []


def get_histograms(scenario,input_folder):
    at_list = ["Proto","StartTime","Dur","Sport","Dir","Dport","State","sTos","dTos","TotPkts","TotBytes","Label"]
    hist_list = dict()
    hist_list['at_list'] = at_list
    for obj in at_list:
        fname = input_folder + obj +scenario+".txt"
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


def to_edge(temp_folder,source, dest):
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


def generate_node(h,r,degree_array,len_deg_ar,TM):
    l = 0
    for each in h:
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
    indeg.append(degree_array[r][len(degree_array[r])-1][0]/len_deg_ar + indeg[0])
    outdeg.append(degree_array[r][len(degree_array[r])-1][1]/len_deg_ar + outdeg[0])
    if int(indeg[0]) == int(indeg[1]):
        indeg = int(indeg[0])
    else:
        indeg = random.randint(int(indeg[0]),int(indeg[1]))
    if int(outdeg[0]) == int(outdeg[1]):
        outdeg = int(outdeg[0])
    else:
        outdeg = random.randint(int(outdeg[0]),int(outdeg[1]))
    if indeg == 0 and outdeg == 0:
        outdeg = 1
    return [l + 1,startrole,indeg,outdeg]


def generate_edge(f,TM,nodes):
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


def add_edge(b,c,temparray,attribute_histograms,srole,drole,mal_role):
    d,warn = attributes(attribute_histograms,srole,drole)
    oth = set_attr(temparray)
    ret = [b,c,d]
    for each in oth:
        for item in each:
            ret.append(item)
    if('otnet' in ret[-1] or 'alicious' in ret[-1]):
        ret[-1] = 'flow = background'
    if(srole == mal_role):
        ret[-1] = 'flow = Botnet Activity'
    return ret,warn


def create_graph(temp_folder,scenario,seed = 0,startpoint = 0):
    fname = scenario.split('.')
    scenario = ''
    count = 1
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
    input_folder += "\\temp\\"
    attribute_histograms = get_histograms(scenario,input_folder)
    #print attribute_histograms['at_list']
    #sys.exit(0)
    mal_role = temp_folder+ "malicious_role" +scenario + ".txt"
    mal_role = open(mal_role,'r')
    for each in mal_role:
        mal_role = int(each)
        break
    name_inputfile = input_folder+ scenario + "." + fname[-1]#name_inputfile = "" + scenario + "." + fname[-1]
    OF = open(name_inputfile, 'r')
    temparray = []
    count = 0
    for each in OF:
        temparray.append(each.split(','))
        count += 1
    if '\n' in temparray[0]:
        temparray[0].remove('\n')

    attribute_histograms['attributes'] = temparray[0]
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
    TA = []
    gc.collect()
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
    gc.collect()
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
    count = 0
    role = 0
    nodes = []
    startpos = []
    while count < roles:
        startpos.append(setstart(count,TM))
        count += 1
    count = 0
    while role < roles:
        nodes.append([])
        while count < a[role]:
            nodes[role].append(generate_node(nodes,role,degree_array,lendegar,TM))
            count += 1
        role += 1
        count = 0
    degree_array = []
    gc.collect()
    count = 1
    for each in nodes:
        for item in each:
            item[0] = count
            count += 1
    count = 0
    edgelist = []
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
    for each in nodes:
        outnodes.append(each[:])
        innodes.append(each[:])
    for each in innodes:
        if len(each) != 0:
            for item in each:
                if int(item[2]) == 0:
                    innodes[count].remove(item)
        count += 1
    count = 0
    for each in nodes:
        if len(each) != 0:
            for item in each:
                if int(item[3]) == 0:
                    outnodes[count].remove(item)
        count += 1
    count = 0
    flag = 0
    warn = False
    warn_flag = False
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
        x,y,z = generate_edge(role,TM,nodes[:])
        if y == 'complete':
            break
        if y != z:
            edge,warn = add_edge(y,z,temparray,attribute_histograms,role,x,mal_role)
            edgelist.append(edge)
        flag = 1
        if warn == True:
            warn_flag = True
        for each in outnodes:
            if len(each) > 0:
                flag = 0
        count += 1
    comm = mpi4py.MPI.COMM_WORLD
    MPI_rank = comm.Get_rank()
    #name_outputfile = "SimulatedGraph/localgen_" + str(MPI_rank)+'.csv'
    name_outputfile = os.path.dirname(os.path.dirname(temp_folder))
    #name_outputfile = name_outputfile + "/SimulatedGraph/localgen_" + str(MPI_rank)+'.csv'
    name_outputfile = name_outputfile + "\\SimulatedGraph\\localgen_" + str(MPI_rank)+'.csv'
    outfile = open(name_outputfile, 'wb')
    topline = ""
    for each in temparray[0]:
        topline = topline + each.strip('\n') + ','
    outfile.write((topline + '\n').encode("utf-8"))
    count = 0
    miscfile = open(temp_folder+'samples.csv','wb')
    i = 0
    for each in edgelist:
        for item in each:
            count += 1
            outfile.write(str(item).encode("utf-8"))
            if(i < 50 and each[-1] == 'flow=Background' and len(each) > 2 and MPI_rank == 0):
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
    LOC = os.path.dirname(os.path.dirname(temp_folder))
    #warn = open(LOC+'/Warning_File.txt',"wb")
    warn = open(LOC+'\\SimulatedGraph\\Warning_File.txt',"w")
    if warn_flag == True:
        warn.write("WARNING\n")
        warn.write("detected edge connection between unanticipated roles or missing histogram\n")
        warn.write("edge attributes have been randomly assigned from existing histograms for any such connections\n")
    else:
        warn.write("generation complete with no warnings")
    print len(edgelist)



if __name__ == "__main__":

    start = time.time()
    #create_graph("CTU_5",startpoint=0)
    TF = "C:\\Users\\Chris\\PycharmProjects\\untitled\\testing\\temp\\"
    create_graph(TF,"5.csv",seed=1)
    print(time.time() - start)
    #comm = mpi4py.MPI.COMM_WORLD
    #MPI_size = comm.Get_size()
    #MPI_rank = comm.Get_rank()