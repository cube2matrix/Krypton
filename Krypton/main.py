#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang Li
# encoding=utf8

from pyspark import SparkConf, SparkContext
import nltk
import string


conf = SparkConf().setMaster("local[*]").setAppName("Krypton")
sc = SparkContext(conf=conf)


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
    return [(transferEncoding(word.lower()), tag) for (word, tag) in tagsPair]


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
        utf8EncodingSentence = transferEncoding(sentence)
        words = tokenizer(utf8EncodingSentence)
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
    line = transferEncoding(line.strip())
    (paperId, title, abstract) = line.split('\t')
    content = title + ' ' + abstract
    sentences = splitIntoSentences(content)
    result = findCompoundNouns(sentences)

    return (paperId, list(set(result)))


def formatCNNode(cnNode):
    word1 = cnNode[0]
    word2 = cnNode[1]
    return word1 + '|' + word2 if word1 < word2 else word2 + '|' + word1


def formatEdge(edgePair):
    n1 = edgePair[0]
    n2 = edgePair[1]

    return "{v1} {v2}".format(v1=n1, v2=n2)


def mapToEdge(pair):
    paperId = pair[0].strip()
    cnNodes = pair[1]

    edgesList = []
    for cnNode in cnNodes:
        word1 = cnNode[0].strip()
        word2 = cnNode[1].strip()
        fCNNode = formatCNNode(cnNode)
        edgesList.append(formatEdge((fCNNode, word1)))
        edgesList.append(formatEdge((fCNNode, word2)))
        edgesList.append(formatEdge((fCNNode, paperId)))

        edgesList.append(formatEdge((word1, fCNNode)))
        edgesList.append(formatEdge((word2, fCNNode)))
        edgesList.append(formatEdge((paperId, fCNNode)))
    return edgesList


def main():

    words = sc.textFile("/Users/darrenxyli/Documents/Krypton/test/data\
/cleaned/test.tsv").cache()

    pair = words.map(seperateEachLine)
    nodeList = pair.map(mapToEdge)
    edges = nodeList.reduce(lambda x, y: x + y)
    edgesRDD = sc.parallelize(edges, 1)
    edgesRDD.saveAsTextFile("/Users/darrenxyli/Documents/Krypton/test/data/cleaned/edges")

    # print edges


if __name__ == '__main__':
    main()
