from multiprocessing.dummy import freeze_support
import ijson
from urllib.request import urlopen
import pandas as pd
import os

stack = []
parserData = open('parser.csv', 'w')
targetDiseasePair = set()

def parsingJSON(parserObj):
    if('{' in parserObj):
        stack.append(parserObj)
    if('}' in parserObj):
        print(stack.pop())
        for prefix, event, value in stack.pop():
            print(value)

def listFiles(path):
    return os.listdir(path)


for filePath in listFiles('.\\Dataset\\Evidence'):
    data = pd.read_json('.\\Dataset\\Evidence\\'+filePath, lines=True)
    data.to_csv('.\\parserData.csv', columns=['diseaseId', 'targetId', 'score'])

readParsedData = pd.read_csv('.\\parserData.csv')
readParsedData.groupby(['diseaseId','targetId']).score.median().to_csv('.\\median.csv')

readParsedData.groupby(['diseaseId','targetId']).score.nlargest(3).to_csv('.\\largestScore.csv')