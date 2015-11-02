#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Xinyang Li
# encoding=utf8

from pyspark import SparkConf, SparkContext
# from pattern.en import ngrams

conf = SparkConf().setMaster("local[*]").setAppName("Krypton").set("spark.executor.memory", "16g")

def test(line):
	line = transferEncoding(line.strip())
	(paperId, title, abstract) = line.split('\t')
	# sentences = splitIntoSentences(abstract)
	# ng = ngrams(abstract, n=2)
	from pattern.en import parse
	ng = parse(abstract)
	return (paperId, ng)
	
def redu(key, value):
	# value = value.strip()
	# (paperId, title, abstract) = line.split('\t')
	# return title.split(' ')
	return value

def transferEncoding(content):
	return content.encode('utf-8')


def splitIntoSentences(content):
    sentencesList = list()
    for sentence in content.split('.'):
        if '?' in sentence:
            sentencesList.extend(sentence.split('?'))
        elif '!' in sentence:
            sentencesList.extend(sentence.split('!'))
        else:
            sentencesList.append(sentence.strip())
    return sentencesList


def findCompoundNouns(content):
	
	utf8EncodingContent = transferEncoding(content)
	sentences = splitIntoSentences(utf8EncodingContent)
	# words = tokenizer(sentences)
	# posTagging()
	# removePunctuation()
	# removeSmallWord()
	# removeNonAlpha()
	# lowerCase()
	# removeStopWord()
	# stemming()



sc = SparkContext(conf = conf, pyFiles=['/Users/darrenxyli/Documents/Krypton/Krypton/pattern.zip'])
words = sc.textFile("/Users/darrenxyli/Documents/Krypton/test/data/cleaned/test.tsv").cache()

pair = words.map(test)
# result = pair.reduceByKey(redu)

print pair.collect()