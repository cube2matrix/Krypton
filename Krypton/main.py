#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang Li
# encoding=utf8

from pyspark import SparkConf, SparkContext
import nltk
import string

conf = SparkConf().setMaster("local[*]").setAppName("Krypton")


def seperateEachLine(line):
    line = transferEncoding(line.strip())
    (paperId, title, abstract) = line.split('\t')
    content = title + ' ' + abstract
    sentences = splitIntoSentences(content)
    result = findCompoundNouns(sentences)

    return (paperId, list(set(result)))


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
    return [(pair1[0], pair2[0]) for (pair1, pair2) in bigramPairs
        if pair1[1] in nounsTypes and pair2[1] in nounsTypes]


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


sc = SparkContext(conf=conf)
words = sc.textFile("/Users/darrenxyli/Documents/Krypton/test/data\
    /cleaned/test_2.tsv").cache()

pair = words.map(seperateEachLine)
nodeList = pair.reduceByKey(lambda x, y: x + y)
nodeList.persist()
