import json
from _collections import defaultdict
import sys
from queue import *
from pyspark import SparkConf, SparkContext
from collections import defaultdict
import time
import pyspark
from itertools import groupby
import os
import math
import io
import sys
import time
from operator import add
time_s = time.time()

os.environ['PYSPARK_PYTHON'] = '/Library/Frameworks/Python.framework/Versions/3.6/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Library/Frameworks/Python.framework/Versions/3.6/bin/python3.6'
os.environ["JAVA_HOME"] = '/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home'

conf = SparkConf().setAppName('task2_2.py').setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

data_file = './power.txt'
#data_file = sys.argv[1]
#out_file_between = sys.argv[2]
#out_file_comm = sys.argv[3]
out_file_between = './betweeness.txt'
out_file_comm = './community.txt'

def calculate(x, adjacent, n):
    q = Queue(maxsize=n)
    visited = []
    levels = {}
    parent = {}
    weight = {}
    q.put(x)
    visited.append(x)
    levels[x] = 0
    weight[x] = 1
    while not q.empty():
        node = q.get()
        child = adjacent[node]

        for i in child:
            if i not in visited:
                q.put(i)
                parent[i] = [node]
                weight[i] = weight[node]
                visited.append(i)
                levels[i] = levels[node]+1
            else:
                if i != x:
                    parent[i].append(node)
                    if levels[node] == levels[i] - 1:
                        weight[i] += weight[node]

    order = []
    count = 0
    for i in visited:
        order.append((i, count))
        count += 1

    reverse = sorted(order, key=(lambda y: y[1]), reverse=True)
    rev_order = []
    node_value = {}
    for i in reverse:
        rev_order.append(i[0])
        node_value[i[0]] = 1

    between = {}

    for j in rev_order:
        if j != x:
            total_w = 0
            for i in parent[j]:
                if levels[i] == levels[j]-1:
                    total_w += weight[i]
            for i in parent[j]:
                if levels[i] == levels[j]-1:
                    src = j
                    dst = i

                    if src < dst:
                        pair = tuple((src, dst))
                    else:
                        pair = tuple((dst, src))
                    if pair not in between.keys():
                        between[pair] = float(node_value[src] * weight[dst]/total_w)
                    else:
                        between[pair] += float(node_value[src] * weight[dst] / total_w)
                    node_value[dst] += float(node_value[src] * weight[dst]/total_w)

    betweenness_list = []
    for key, value in between.items():
        temp = [key, value]
        betweenness_list.append(temp)
    return betweenness_list

def find(x, list_e):
    res = []
    for el in list_e:
        if el[0] == x:
            res.append(el[1])
        if el[1] == x:
            res.append(el[0])
    res = list(set(res))
    return res

def bfs(root, adj_matrix, n_vertices):
    visited = []
    edges = []

    q = Queue(maxsize=n_vertices)
    q.put(root)
    visited.append(root)

    while not q.empty():
        fron = q.get()
        neighbors = adj_matrix[fron]

        for j in neighbors:
            if j not in visited:
                q.put(j)
                visited.append(j)

            pair = sorted((fron, j))
            if pair not in edges:
                edges.append(pair)

    return (visited, edges)

def remove_comp(remainder, comp):

    component_v = comp[0]

    for v in component_v:
        del remainder[v]
    for j in remainder.keys():
        adj_list = remainder[j]
        for v in component_v:
            if v in adj_list:
                adj_list.remove(j[1])
        remainder[j] = adj_list

    return remainder

def empty(adj_matrix):
    if len(adj_matrix) == 0:
        return True
    else:
        for j in adj_matrix.keys():
            l = adj_matrix[j]
            if len(l) != 0:
                return False
            else:
                pass
        return True

def get_connected(adj_matrix):
    connected = []
    remainder = adj_matrix

    while not empty(remainder):
        vertices = []

        for key, value in remainder.items():
            vertices.append(key)

        vertices = list(set(vertices))
        root = vertices[0]
        comp = bfs(root, adj_matrix, len(vertices))
        connected.append(comp)
        remainder = remove_comp(remainder, comp)
    return connected

def calculate_Q(adjacent, components, e):
    modularity = 0
    for c in components:
        vertices = c[0]

        Aij = 0
        for i in vertices:
            for j in vertices:
                Aij = 0
                l = adjacent[str(i)]
                if j in l:
                    Aij = 1

                ki = len(adjacent[i])
                kj = len(adjacent[j])

                modularity = modularity + Aij - (ki*kj)/(2*e)
    modularity = modularity/(2*e)
    return modularity

def remove(adj_matrix, first_edge):
    if first_edge[0] in adj_matrix.keys():
        lis = adj_matrix[first_edge[0]]
        if first_edge[1] in lis:
            lis.remove(first_edge[1])

    if first_edge[1] in adj_matrix.keys():
        lis = adj_matrix[first_edge[1]]
        if first_edge[0] in lis:
            lis.remove(first_edge[0])
    return adj_matrix

def build(components):
    ans = {}
    for c in components:
        c_e = c[1]

        for i in c_e:
            if i[0] in ans.keys():
                ans[i[0]].append(i[1])
            else:
                ans[i[0]] = [i[1]]
            if i[1] in ans.keys():
                ans[i[1]].append(i[0])
            else:
                ans[i[1]] = [i[0]]
    return ans

read_data = sc.textFile(data_file)
rdd = read_data.map(lambda x: x.split(' '))
print(rdd)

rdd_v = rdd.flatMap(lambda x: [(x[0]), (x[1])]).distinct()
list_v = rdd_v.collect()
print(list_v)
n = len(list_v)

rdd_e = rdd.map(lambda x: (x[0], x[1]))
list_e = rdd_e.collect()
print(list_e)

adjacent = rdd_v.map(lambda x: (x, find(x, list_e))).collectAsMap()
print(adjacent)

betweeness = rdd_v.flatMap(lambda x: calculate(x, adjacent, n)).reduceByKey(lambda x, y: (x+y)).map(lambda x: (x[0], float(x[1]/2)))\
    .sortByKey().map(lambda x: (x[1], x[0])).sortByKey(ascending=False).map(lambda x: (x[1], x[0]))

first_edge = betweeness.take(1)[0][0]
e = len(list_e)

adj_matrix = adjacent.copy()
components = get_connected(adj_matrix)
mod = calculate_Q(adjacent, components, e)
adj_matrix = adjacent.copy()

high = -1
communitites = []
con = 0
time_s_1 = time.time()
while 1:
    adj_matrix = remove(adj_matrix, first_edge)
    components = get_connected(adj_matrix)
    mod = calculate_Q(adjacent, components, e)
    #if mod < high:
    con += 1
    adj_matrix = build(components)
    temp = []
    for j in adj_matrix.keys():
        temp.append(j)
    temp = list(set(temp))
    vertex_rdd = sc.parallelize(temp)
    betweeness_temp = vertex_rdd.flatMap(lambda x: calculate(x, adj_matrix, n)).reduceByKey(lambda x, y: (x+y))\
    .map(lambda x: (x[0], float(x[1]/2))).sortByKey().map(lambda x: (x[1], x[0])).sortByKey(ascending=False).map(lambda x: (x[1], x[0]))

    if mod >= high:
        high = mod
        communitites = components
    #if con == 200:
        #break
    if time.time() - time_s_1 > 356:
        break
    if empty(adj_matrix):
        break
    first_edge = betweeness_temp.take(1)[0][0]

time_e_1 = time.time()
sorted_comm = []
for j in communitites:
    temp_1 = sorted(j[0])
    sorted_comm.append((temp_1, len(temp_1)))

sorted_comm.sort()
sorted_comm.sort(key=lambda x: x[1])

c = 0
o = open(out_file_between, 'w')
for i in betweeness.collect():
    c += 1
    o.write(str(i[0]) +", " + str(i[1]))
    if c < len(betweeness.collect()):
        o.write("\n")
o.close()

o = open(out_file_comm, 'w')
c = 0
for i in sorted_comm:
    c += 1
    s = str(i[0]).replace("[", "").replace("]", "")
    o.write(s)
    if c < len(sorted_comm):
        o.write("\n")
o.close()

print(time_e_1 - time_s_1)
time_e = time.time()
print(time_e - time_s)
