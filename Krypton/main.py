#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang Li
# encoding=utf8

from pyspark import SparkConf, SparkContext
import nltk
import string


conf = SparkConf().setAppName("Krypton")
sc = SparkContext(conf=conf)

nltk.download("punkt")
nltk.download("maxent_treebank_pos_tagger")

def transferEncoding(content):
    return content.encode('utf-8')


def splitIntoSentences(content):
    return nltk.sent_tokenize(content)


def tokenizer(sentence):
    return nltk.word_tokenize(sentence)


def posTagging(words):
    return nltk.pos_tag(words)


def removePunctuation(tagsPair):
    return [(word, tag) for (word, tag) in tagsPair if not isPunctuation(word)]


def isPunctuation(mark):
    return mark in string.punctuation


def removeSmallWord(tagsPair):
    return [(word, tag) for (word, tag) in tagsPair if len(word) >= 3]


def toLowerCase(tagsPair):
    return [(word.lower(), tag) for (word, tag) in tagsPair]


def removeStopWord(tagsPair):
    from nltk.corpus import stopwords
    stop = stopwords.words('english')
    return [(word, tag) for (word, tag) in tagsPair if word not in stop]


def stemming(tagsPair):
    from nltk.stem import WordNetLemmatizer
    wnl = WordNetLemmatizer()
    return [(wnl.lemmatize(word), tag) for (word, tag) in tagsPair]


def getCNPair(tagsPair):
    nounsTypes = ['NN', 'NNS', 'NNP', 'NNPS']
    bigramPairs = nltk.bigrams(tagsPair)
    return [(pair1[0], pair2[0]) for (pair1, pair2) in bigramPairs if pair1[1] in nounsTypes and pair2[1] in nounsTypes]


def findCompoundNouns(sentences):

    CNList = []

    for sentence in sentences:
        # utf8EncodingSentence = transferEncoding(sentence)
        words = tokenizer(sentence)
        pairs = posTagging(words)
        pairs = removePunctuation(pairs)
        pairs = removeSmallWord(pairs)
        pairs = toLowerCase(pairs)
        pairs = removeStopWord(pairs)
        pairs = stemming(pairs)
        for pair in getCNPair(pairs):
            CNList.append(pair)

    return CNList


def seperateEachLine(line):
    line = line.strip()
    (paperId, title, abstract) = line.split('\t')
    content = title + ' ' + abstract

    return (paperId, content)


def extractCN(pair):
    content = pair[1]
    paperId = pair[0]
    sentences = splitIntoSentences(content)
    cns = findCompoundNouns(sentences)

    return (paperId, list(set(cns)))


def generateNodeID(pair):
    paperId = pair[0].strip()
    cnNodes = pair[1]

    nodes = []
    nodes.append(paperId)
    for cnNode in cnNodes:
        word1 = cnNode[0].strip()
        word2 = cnNode[1].strip()
        fCNNode = formatCNNode(cnNode)
        nodes.append(word1)
        nodes.append(word2)
        nodes.append(fCNNode)
    return list(set(nodes))


def formatCNNode(cnNode):
    word1 = cnNode[0]
    word2 = cnNode[1]
    if word1 < word2:
        return "{v1}|{v2}".format(v1=word1, v2=word2)
    else:
        return "{v1}|{v2}".format(v1=word2, v2=word1)


def formatEdge(edgePair):
    n1 = edgePair[0]
    n2 = edgePair[1]

    return "{v1} {v2}".format(v1=n1, v2=n2)


def mapToEdge(mapBrdCstDict, pair):
    paperId = pair[0].strip()
    cnNodes = pair[1]

    edgesList = []
    idNameMap = mapBrdCstDict.value
    paperRddId = idNameMap[paperId]

    for cnNode in cnNodes:
        word1 = cnNode[0].strip()
        word2 = cnNode[1].strip()
        fCNNode = formatCNNode(cnNode)

        word1Id = idNameMap[word1]
        word2Id = idNameMap[word2]
        cnNodeId = idNameMap[fCNNode]

        edgesList.append(formatEdge((cnNodeId, word1Id)))
        edgesList.append(formatEdge((cnNodeId, word2Id)))
        edgesList.append(formatEdge((cnNodeId, paperRddId)))

        edgesList.append(formatEdge((word1Id, cnNodeId)))
        edgesList.append(formatEdge((word2Id, cnNodeId)))
        edgesList.append(formatEdge((paperRddId, cnNodeId)))
    return edgesList


def main():

    words = sc.textFile("/Users/darrenxyli/Documents/Krypton/test/data/cleaned/test.tsv").cache()

    records = words.map(seperateEachLine)
    pair = records.map(extractCN)

    pair.persist()

    # generate ID for each node
    nodeIDs = pair.map(generateNodeID)
    nodes = nodeIDs.reduce(lambda x, y: x + y)
    nodesRDD = sc.parallelize(nodes, 1)
    nodesRDD = nodesRDD.zipWithUniqueId()

    # save ID-Name mapping into file
    formatNodeID = nodesRDD.map(lambda pair: "{id},{name}".format(id=pair[1], name=pair[0]))
    formatNodeID.saveAsTextFile("/Volumes/extend1/amazon/data/pubmed/603/nodes")

    # broadcast map relationship dictionary
    V = sc.broadcast(nodesRDD.collectAsMap())

    # create edge list
    edgeList = pair.map(lambda item: mapToEdge(V, item))
    edges = edgeList.reduce(lambda x, y: x + y)
    edgesRDD = sc.parallelize(edges, 1)
    edgesRDD.saveAsTextFile("/Volumes/extend1/amazon/data/pubmed/603/edges")


if __name__ == '__main__':
    main()
