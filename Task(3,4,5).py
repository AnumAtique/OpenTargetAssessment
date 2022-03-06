from unicodedata import name
import dask.dataframe as df
import os
import cx_Oracle
import pandas as pd
from pyparsing import null_debug_action
import json
from multiprocessing import Process

checkFile = False
def listFiles(path):
    return os.listdir(path)

def createConnection(dbPath):
    con = cx_Oracle.connect(dbPath)
    return con

def addTableData(dbPath, filePath):
    connection = createConnection(dbPath)
    cursor = connection.cursor()
    data = pd.read_csv(filePath, on_bad_lines='skip')
    frame = pd.DataFrame(data)
    mergeData = []
    query = '''Insert into IPAPP.DISEASE(Symbol, Name, Score) values(:a, :b, :d)'''
    i=0
    frame = frame.fillna(0)
    for row in frame.itertuples():
        mergeData.append([str(row[5]), str(row[33]), str(row[3])])
    cursor.executemany(query, mergeData)
    connection.commit()

def addHeaders(path):
    headers = 'diseaseId, targetId, score, id_x, approvedSymbol, biotype, transcriptIds, genomicLocation, approvedName, go, synonyms_x, symbolSynonyms, nameSynonyms, functionDescriptions, subcellularLocations, obsoleteSymbols, obsoleteNames, constraint, proteinIds, dbXrefs, homologues, tractability, pathways, targetClass, safetyLiabilities, hallmarks, chemicalProbes, alternativeGenes, id_y, code, dbXRefs, description, name, parents, synonyms_y, ancestors, descendants, children, therapeuticAreas, ontology, indirectLocationIds, obsoleteTerms, directLocationIds'
    if not os.path.isfile(path):
        open(path, 'w', encoding='UTF8', newline='').write(str(headers)+'\n')
    checkFile = True   

def jsonReader(path):
    return df.read_json(path)

def tableToJson(dbPath):
    connection = createConnection(dbPath)
    cursor = connection.cursor()
    queryData = cursor.execute("Select * from IPAPP.DISEASE order by score asc")
    r = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    cursor.connection.close()
    return (r[0] if r else None) if False else r

def joinDatasets(targetPath, evidenceFile):
    targetData = jsonReader('.\\Dataset\\Targets\\' + targetPath)
    for diseaseFilePath in listFiles('.\\Dataset\\Disease'):
        diseaseData = df.read_json('.\\Dataset\\Disease\\'+diseaseFilePath)
        joinedFile = evidenceFile.merge(targetData,  left_on='targetId', right_on='id').merge(diseaseData, left_on='diseaseId', right_on='id')
        if not checkFile:
            addHeaders('.\\TargetDisease.csv')
        joinedFile.to_csv('.\\TargetDisease.csv', mode='a', index=False, header=None)

evidenceList = df.read_csv('.\\median.csv')



def main():

    for filePath in listFiles('.\\Dataset\\Targets'):
        p = Process(target=joinDatasets, args=(filePath, evidenceList))
        p.start()
    p.join()

    addTableData('ipapp/remittance@localhost:8004/remdbuat', '.\\TargetDisease.csv')
    jsonFile = open(".\\DiseaseTarget.json", 'w')
    jsonFile.write(json.dumps(tableToJson('ipapp/remittance@localhost:8004/remdbuat')))


if __name__ == "__main__":
    main()