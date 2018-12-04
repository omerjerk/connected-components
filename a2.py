#!/usr/bin/python

'''
First Name - Mohammad
Last Name - Umair
Ubit Name - m39
'''

from pyspark import SparkConf, SparkContext
import sys

def read_data(line):
    l = line.split(" ")
    return (int(l[0]), int(l[1]))

def debug(line):
    print(line)
    return line

def largeStarMap(tup):
    return [(tup[0], tup[1]), (tup[1], tup[0])]

def largeStarReduce(tup):
    node = int(tup[0])
    # data = [int(i[0]) for i in tup[1]]
    # data.append(node)
    data = tup[1]
    m = min(data)
    m = min(m, node)
    to_return = []
    for v in data:
        if v > node:
            to_return.append((v, m))
    return to_return

def smallStarMap(tup):
    u = tup[0]
    v = tup[1]

    if (u > v):
        return (u, v)
    else:
        return (v, u)

def smallStarReduce(tup):
    node = tup[0]
    data = [i for i in tup[1]]
    data.append(node)
    m = min(data)
    to_return = []
    for v in data:
        if v != m:
            to_return.append((v, m))
    return to_return

def outputEdges(tup):
    key = tup[0]
    val = tup[1]
    return str(key) + " " + str(val)

def check_convergence(rdd1, rdd2):
    diff_rdd = rdd.subtract(prev_rdd).union(prev_rdd.subtract(rdd))
    if diff_rdd.count() == 0:
        return True
    return False

if __name__ == "__main__":
    conf = SparkConf().setAppName("ComponentCount")
    sc = SparkContext(conf = conf)
    rdd = sc.textFile(sys.argv[1])
    rdd = rdd.map(read_data)
    while True:
        while True:
            prev_rdd = rdd
            rdd = rdd.flatMap(largeStarMap).groupByKey().flatMap(largeStarReduce)
            #print("checking for large star convergence")
            if check_convergence(prev_rdd, rdd):
                break
        prev_rdd = rdd
        rdd = rdd.map(smallStarMap).groupByKey().flatMap(smallStarReduce)
        #print("checking for small star convergence")
        if check_convergence(prev_rdd, rdd):
            break
    
    vals_rdd = rdd.values().distinct()
    print("components = " + str(vals_rdd.count()))
    rdd = rdd.union(vals_rdd.map(lambda k: (k, k)))
    rdd = rdd.map(outputEdges)
    print("count = " + str(rdd.count()))

    rdd.saveAsTextFile("output")
