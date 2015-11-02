#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import xmltodict
import json


def listXmlPath(path_init):
    """
    List full xml path under given directory
    """
    fullpath = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(path_init)) for f in fn]
    xmlList = [folder for folder in fullpath if os.path.splitext(folder)[-1] == '.xml']
    return xmlList

def listNxmlPath(path_init):
    """
    List full xml path under given directory
    """
    fullpath = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(path_init)) for f in fn]
    nxmlList = [folder for folder in fullpath if os.path.splitext(folder)[-1] == '.nxml']
    return nxmlList

def xmlConvDict(xmlpath):
    '''
    Convert xml content to dictionary type
    '''
    try:
        oriCtn = open(xmlpath).read()
        xmlDict = json.loads(json.dumps(xmltodict.parse(oriCtn), indent=4))
        return xmlDict
    except:
        raise Exception("It was not able to read a path, a file-like object, or a string as an XML")
