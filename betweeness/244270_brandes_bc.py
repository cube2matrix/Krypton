#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang Li
# encoding=utf8

from pyspark import SparkConf, SparkContext
from collections import deque
import timeit

conf = SparkConf().setAppName("Krypton")
sc = SparkContext(conf=conf)


def buildGraph():
    V = []

    num_lines = sum(1 for line in open('/gpfs/courses/cse603/students/xli66/603/graph/3904lines/nodes/part-00000'))
    for x in xrange(num_lines):
        V.append(x)

    A = dict((w, []) for w in V)

    with open('/gpfs/courses/cse603/students/xli66/603/graph/3904lines/edges/part-00000') as f :
        for line in f:
            id1 = int(line.split(" ")[0])
            id2 = int(line.split(" ")[1])
            A[id1].append(id2)
            A[id2].append(id1)

    return (V, A)


def main():
    loadS = timeit.default_timer()
    (V, A) = buildGraph()
    loadE = timeit.default_timer()
    print "Load: "
    print loadE - loadS

    start = timeit.default_timer()
    brandes(V, A)
    stop = timeit.default_timer()

    print "Run: "
    print stop - start


def single_source_dijkstra_path_basic(s, Vb, Ab):
    V = Vb.value
    A = Ab.value
    S = []
    P = dict((w, []) for w in V)
    g = dict((t, 0.0) for t in V)
    g[s] = 1.0
    d = dict((t, -1) for t in V)
    d[s] = 0

    Q = deque([])
    Q.append(s)
    while Q:
        v = Q.popleft()
        S.append(v)
        for w in A[v]:
            if d[w] < 0:
                Q.append(w)
                d[w] = d[v] + 1
            if d[w] == d[v] + 1:
                g[w] = g[w] + g[v]
                P[w].append(v)
    C = accumulate_basic(V, P, g, S, s)
    return C


def accumulate_basic(V, P, g, S, s):
    C = 0.0
    e = dict((v, 0.0) for v in V)
    while S:
        w = S.pop()
        for v in P[w]:
            e[v] = e[v] + (g[v]/g[w]) * (1 + e[w])
            if w != s:
                C += e[w]
    return C


def brandes(V, A):
    Vb = sc.broadcast(V)
    Ab = sc.broadcast(A)
    nodes = sc.parallelize(V, 64)
    nodes.map(lambda s : single_source_dijkstra_path_basic(s, Vb, Ab)).collect()


# def brandes(V, A):
#     "Compute betweenness centrality in an unweighted graph."
#     # Brandes algorithm
#     # see http://www.cs.ucc.ie/~rb4/resources/Brandes.pdf
#     C = dict((v, 0.0) for v in V)
#     for s in V:
#         S = []
#         P = dict((w, []) for w in V)
#         g = dict((t, 0.0) for t in V)
#         g[s] = 1.0
#         d = dict((t, -1) for t in V)
#         d[s] = 0
#         Q = deque([])
#         Q.append(s)
#         while Q:
#             v = Q.popleft()
#             S.append(v)
#             for w in A[v]:
#                 if d[w] < 0:
#                     Q.append(w)
#                     d[w] = d[v] + 1
#                 if d[w] == d[v] + 1:
#                     g[w] = g[w] + g[v]
#                     P[w].append(v)
#         e = dict((v, 0) for v in V)
#         while S:
#             w = S.pop()
#             for v in P[w]:
#                 e[v] = e[v] + (g[v]/g[w]) * (1 + e[w])
#                 if w != s:
#                     C[w] = C[w] + e[w]
#     return C

if __name__ == '__main__':
    main()
