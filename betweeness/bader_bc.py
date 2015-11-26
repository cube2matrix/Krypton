#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang Li
# encoding=utf8

from pyspark import SparkConf, SparkContext
from collections import deque
import sys
import re
import timeit

conf = SparkConf().setAppName("Krypton")
sc = SparkContext(conf=conf)


def parseId(line):
    parts = re.split(r',', line)
    return int(parts[0])


def parseLink(line):
    vertex = re.split(r' ', line)
    for x in xrange(2):
        yield (int(vertex[x % 2]), int(vertex[(x+1) % 2]))


def buildGraph(node_file, edge_file):
    node_lines = sc.textFile(node_file, 1)
    V = node_lines.map(lambda line: parseId(line)).distinct().cache()

    edge_lines = sc.textFile(edge_file, 1)
    A = edge_lines.flatMap(lambda line: parseLink(line)).groupByKey().cache()

    return (V, A)


def main():
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: bader_bc <nodeFile> <edgeFile>"
        exit(-1)

    node_file_path = sys.argv[1]
    edge_file_path = sys.argv[2]

    loadS = timeit.default_timer()
    (V, A) = buildGraph(node_file_path, edge_file_path)
    loadE = timeit.default_timer()

    BC = V.map(lambda v : (v, 0.0)).collectAsMap()

    for s in V.collect():

        # Initialization
        Succ = V.map(lambda v: (v, [])).collectAsMap()
        g = V.map(lambda v: (v, 0.0)).collectAsMap()
        d = V.map(lambda v: (v, -1)).collectAsMap()

        g[s] = 1
        d[s] = 0

        S = {}
        phase = 0
        Q = deque([])
        Q.append(s)
        S[phase] = Q
        count = sc.accumulator(1)

        # Graph Traversal
        while count.value > 0:
            count.add(-count.value)

            ###
            ###

            phase = phase + 1
        phase = phase - 1

        # Dependency Accumulation
        e = V.map(lambda v: (v, 0.0)).collectAsMap()
        phase = phase - 1
        for w in S[phase]:
            dsw = 0
            sw = g[w]
            for v in Succ[w]:
                dsw = dsw + sw/g[v] * (1+e[v])
            e[v] = dsw
            if w != s:
                BC[w] = BC[w] + dsw

    # print A
    print "Load: "
    print loadE - loadS


if __name__ == '__main__':
    main()
