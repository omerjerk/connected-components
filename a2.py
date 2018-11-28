#!/usr/bin/python

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

if __name__ == "__main__":
    conf = SparkConf().setAppName("ComponentCount")
    sc = SparkContext(conf = conf)
    rdd = sc.textFile(sys.argv[1])
    rdd = rdd.map(read_data).map(debug)
    rdd.count()
    rdd = rdd.flatMap(largeStarMap).groupByKey().flatMap(largeStarReduce)
    print("=======")
    rdd = rdd.map(smallStarMap).groupByKey().flatMap(smallStarReduce).map(debug)
    print(rdd.count())

    # rdd.saveAsTextFile("file:/home/omerjerk/study/pdp/A2/output_data.txt")
