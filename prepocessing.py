import sys
from bs4 import BeautifulSoup 
import urllib.request 
import enchant
import nltk
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import json
import string
import math

class Preprocessing:
    dictEnglish = enchant.Dict("en_US")
    stopWords = stopwords.words('english')
    stemmer = PorterStemmer()
    metaTF = []
    totalDocsTF = 0
    dictionaryIDF = {}
    umbral = 0.5

    files = ["TFIDF/2gm-0006", "TFIDF/2gm-0007", "TFIDF/2gm-0008","TFIDF/2gm-0009", "TFIDF/2gm-0010","TFIDF/2gm-0011",
        "TFIDF/2gm-0012", "TFIDF/2gm-0013", "TFIDF/2gm-0014", "TFIDF/2gm-0015", "TFIDF/2gm-0016", "TFIDF/2gm-0017", "TFIDF/2gm-0018",
        "TFIDF/2gm-0019", "TFIDF/2gm-0020", "TFIDF/2gm-0021", "TFIDF/2gm-0022", "TFIDF/2gm-0023", "TFIDF/2gm-0024", "TFIDF/2gm-0025",
        "TFIDF/2gm-0026", "TFIDF/2gm-0027", "TFIDF/2gm-0028", "TFIDF/2gm-0029", "TFIDF/2gm-0030", "TFIDF/2gm-0031"]

    def process(self, url):
        page = self.readUrl(url)
        englishWords = []
        for word in page:
            if self.isEnglishWordNet(word):
                englishWords.append(word)

        validWords = []
        for word in englishWords:
            if not self.isStopWord(word):
                validWords.append(word)

        stematizeWords = []
        for word in validWords:
            stematizeWords.append(self.stematize(word))

        wc = self.wordCount(stematizeWords)
        sortedWC = sorted(wc, key=lambda x: x[1], reverse=True)
        for item in sortedWC:
            print(str(item[0]) + ':' + str(item[1]))

    def readUrl(self, url):
        res = urllib.request.urlopen(url)
        html = res.read()
        soup = BeautifulSoup(html,"html5lib") 
        text = soup.get_text(strip=True) 
        tokens = [t for t in text.split()] 
        return tokens

    def wordCount(self, wordList):
        wordMap = []
        for key,val in nltk.FreqDist(wordList).items():
            wordMap.append( (key,val) )
        return wordMap

    def isEnglishEnchant(self, word):
        return dictEnglish.check(word)

    def isEnglishWordNet(self, word):
        if not wordnet.synsets( word ):
            return False
        else:
            return True
    
    def isStopWord(self, word):
        if word in self.stopWords:
            return True
        else:
            return False
    
    def stematize(self, word):
        return self.stemmer.stem(word)

    def isNumber(self, word):
        try:
            x = float(word)
            return True
        except ValueError:
            return False

    def isValidWord(self, word: string):
        if (not word.isalpha()):
            return False
        if (not self.isEnglishWordNet(word)):
            return False
        if (self.isStopWord(word)):
            return False
        return True
        
    def cleanData(self, fileIn, fileOut):
        print(fileIn)
        try:  
            fpIn = open(fileIn, "r")
            fpOut = open(fileOut, "w")
            line = fpIn.readline().lower()
            while (line):
                words = line.split()
                if ( len(words) >= 3 and self.isValidWord(words[0]) and self.isValidWord(words[1]) ):
                    fpOut.write(line)
                line = fpIn.readline().lower()
        finally:
            fpIn.close()
            fpOut.close()

    def calculateTF(self, frequency, max):
        TF = []
        for freq in frequency:
            TF.append(freq/max)
        return TF

    def calculateIDF(self, total, frequency):
        IDF = []
        for freq in frequency:
            IDF.append(math.log2(total/freq))

    def generateDF(self, fileIn, fileOut):
        print(fileIn)
        max = 1
        prevWord = '' #docID
        features = []
        freq = []
        toralDocs = 0
        try:  
            fpIn = open(fileIn, "r")
            fpOut = open(fileOut, "w")
            line = fpIn.readline()
            while (line):
                max = 1
                words = line.split()
                prevWord = words[0]
                freq = []
                features = []
                toralDocs += 1
                while (line and prevWord == words[0]):
                    features.append( words[1] )
                    tempFreq = int(words[2])
                    freq.append( tempFreq )
                    if ( tempFreq > max ):
                        max = tempFreq
                    line = fpIn.readline()
                    words = line.split()
                TF = self.calculateTF(freq, max)
                fpOut.write(prevWord + "\n")
                fpOut.write(str(len(TF)) + "\n")
                index = 0
                for feat in features:
                    fpOut.write(feat + " " + str(TF[index]) + "\n")
                    index += 1
            self.metaTF.append(fileOut + " " + str(toralDocs))
        finally:
            fpIn.close()
            fpOut.close()

    def saveMetaTF(self):
        try:
            meta = open("TF/meta", "w")
            for item in self.metaTF:
                meta.write(item + "\n")
        finally:
            meta.close()

    def readMetaTF(self):
        try:
            meta = open("TF/meta", "r")
            line = meta.readline()
            words = line.split()
            while(line):
                if (len(words) >= 1):
                    tam = int(words[1])
                    self.totalDocsTF += tam 
                line = meta.readline()
                words = line.split()
            print(self.totalDocsTF)
        finally:
            meta.close

    def generateDictionaryIDF(self, fileIn):
        print(fileIn)
        try:
            fpIn = open(fileIn, "r")
            line = fpIn.readline()
            words = line.split()
            while (line):
                if words[1] in self.dictionaryIDF:
                    self.dictionaryIDF[words[1]] +=1
                else:
                    self.dictionaryIDF[words[1]] = 1
                line = fpIn.readline()
                words = line.split()
        finally:
            fpIn.close()

    def saveDictionaryIDF(self):
        try:
            fpOut = open("IDF/dictionary", "w")
            for key, value in self.dictionaryIDF.items():
                idf = math.log2(self.totalDocsTF / value)
                fpOut.write(key + " " + str(idf) + "\n")
        finally:
            fpOut.close()

    def loadDicionaryIDF(self):
        try:
            fpIn = open("IDF/dictionary", "r")
            line = fpIn.readline()
            words = line.split()
            while(line):
                self.dictionaryIDF[words[0]] = float(words[1])
                line = fpIn.readline()
                words = line.split()
        finally:
            fpIn.close()

    def generateTFIDF(self, fileIn, fileOut):
        print(fileIn)
        try:
            fpIn = open(fileIn, "r")
            fpOut = open(fileOut, "w")
            head = fpIn.readline()
            headSplit = head.split()
            while(head):
                tam = int(fpIn.readline().rstrip("\n"))
                fpOut.write(headSplit[0] + " " + str(tam) + "\n")
                while(tam > 0):
                    line = fpIn.readline()
                    words = line.split()
                    tf = float(words[1])
                    idf = self.dictionaryIDF[words[0]]
                    tfidf =  tf * idf
                    fpOut.write(words[0] + " " + str(tfidf) + "\n")
                    tam -= 1
                head = fpIn.readline()
                headSplit = head.split()
        finally:
            fpIn.close()
            fpOut.close()

    def find(self, wordToFind, file, vector, weigth):
        try:
            fpIn = open(file, "r")
            head = fpIn.readline()
            headSplit = head.split()
            while(head):
                tam = int(headSplit[1])
                if (headSplit[0] == wordToFind):
                    while(tam > 0):
                        line = fpIn.readline()
                        words = line.split()
                        vector.append(words[0])
                        weigth.append(float(words[1]))
                        tam -= 1
                    fpIn.close()
                    return True
                while(tam > 0):
                    line = fpIn.readline()
                    tam -= 1
                head = fpIn.readline()
                headSplit = head.split()
        finally:
            fpIn.close()
        return False

    def findInAll(self, word, vector, weigth):
        for item in self.files:
            print("buscando en " + item + " ...")
            res = self.find(word, item, vector, weigth)
            if ( res == True ):
                return True
        return False
    
    def calculateSimilitude(self, file, myVector, myWeigth, vectorRes, similitudeRes):
        try:
            fpIn = open(file, "r")
            head = fpIn.readline()
            headSplit = head.split()
            while(head):
                tam = int(headSplit[1])
                vector = []
                weigth = []
                while(tam > 0):
                    line = fpIn.readline()
                    words = line.split()
                    vector.append(words[0])
                    weigth.append(float(words[1]))
                    tam -= 1
                similarPercent = self.compareVector(myVector, myWeigth, vector, weigth)
                if (similarPercent > self.umbral):
                    vectorRes.append(headSplit[0])
                    similitudeRes.append(similarPercent)
                head = fpIn.readline()
                headSplit = head.split()
        finally:
            fpIn.close()

    def compareVector(self, vector1, weigth1, vector2, weigth2):
        x = 0
        y = 0
        arriba = 0.0
        abajo1 = 0.0
        abajo2 = 0.0
        for item in weigth1:
            abajo1 += item * item
        for item in weigth2:
            abajo2 += item * item
        while( x < len(vector1) and y < len(vector2) ):
            if (vector1[x] == vector2[y]):
                arriba += weigth1[x] * weigth2[y]
                x += 1
                y += 1
            elif (vector1[x] < vector2[y]):
                x += 1
            else:
                y += 1
        # print (str(arriba) + " " + str(abajo1) + " " + str(abajo2) + "\n")
        return arriba / ( math.sqrt(abajo1) * math.sqrt(abajo2) )

    def getSimilarInAll(self, vector, weigth, vectorRes, similarRes):
        for item in self.files:
            print("comparando en " + item + " ...")
            preprocessing.calculateSimilitude(item, vector, pesos, vectorRes, similarRes)


preprocessing = Preprocessing()

""" codigo para limpiar la data"""
# preprocessing.cleanData("CorpusGoogle/2gm-0006", "Corpus/2gm-0006")
# preprocessing.cleanData("CorpusGoogle/2gm-0007", "Corpus/2gm-0007")
# preprocessing.cleanData("CorpusGoogle/2gm-0008", "Corpus/2gm-0008")
# preprocessing.cleanData("CorpusGoogle/2gm-0009", "Corpus/2gm-0009")
# preprocessing.cleanData("CorpusGoogle/2gm-0010", "Corpus/2gm-0010")
# preprocessing.cleanData("CorpusGoogle/2gm-0011", "Corpus/2gm-0011")
# preprocessing.cleanData("CorpusGoogle/2gm-0012", "Corpus/2gm-0012")
# preprocessing.cleanData("CorpusGoogle/2gm-0013", "Corpus/2gm-0013")
# preprocessing.cleanData("CorpusGoogle/2gm-0014", "Corpus/2gm-0014")
# preprocessing.cleanData("CorpusGoogle/2gm-0015", "Corpus/2gm-0015")
# preprocessing.cleanData("CorpusGoogle/2gm-0016", "Corpus/2gm-0016")
# preprocessing.cleanData("CorpusGoogle/2gm-0017", "Corpus/2gm-0017")
# preprocessing.cleanData("CorpusGoogle/2gm-0018", "Corpus/2gm-0018")
# preprocessing.cleanData("CorpusGoogle/2gm-0019", "Corpus/2gm-0019")
# preprocessing.cleanData("CorpusGoogle/2gm-0020", "Corpus/2gm-0020")
# preprocessing.cleanData("CorpusGoogle/2gm-0021", "Corpus/2gm-0021")
# preprocessing.cleanData("CorpusGoogle/2gm-0022", "Corpus/2gm-0022")
# preprocessing.cleanData("CorpusGoogle/2gm-0023", "Corpus/2gm-0023")
# preprocessing.cleanData("CorpusGoogle/2gm-0024", "Corpus/2gm-0024")
# preprocessing.cleanData("CorpusGoogle/2gm-0025", "Corpus/2gm-0025")
# preprocessing.cleanData("CorpusGoogle/2gm-0026", "Corpus/2gm-0026")
# preprocessing.cleanData("CorpusGoogle/2gm-0027", "Corpus/2gm-0027")
# preprocessing.cleanData("CorpusGoogle/2gm-0028", "Corpus/2gm-0028")
# preprocessing.cleanData("CorpusGoogle/2gm-0029", "Corpus/2gm-0029")
# preprocessing.cleanData("CorpusGoogle/2gm-0030", "Corpus/2gm-0030")
# preprocessing.cleanData("CorpusGoogle/2gm-0031", "Corpus/2gm-0031")


""" codigo para generar DFs """
# preprocessing.generateDF("Corpus/2gm-0006", "TF/2gm-0006")
# preprocessing.generateDF("Corpus/2gm-0007", "TF/2gm-0007")
# preprocessing.generateDF("Corpus/2gm-0008", "TF/2gm-0008")
# preprocessing.generateDF("Corpus/2gm-0009", "TF/2gm-0009")
# preprocessing.generateDF("Corpus/2gm-0010", "TF/2gm-0010")
# preprocessing.generateDF("Corpus/2gm-0011", "TF/2gm-0011")
# preprocessing.generateDF("Corpus/2gm-0012", "TF/2gm-0012")
# preprocessing.generateDF("Corpus/2gm-0013", "TF/2gm-0013")
# preprocessing.generateDF("Corpus/2gm-0014", "TF/2gm-0014")
# preprocessing.generateDF("Corpus/2gm-0015", "TF/2gm-0015")
# preprocessing.generateDF("Corpus/2gm-0016", "TF/2gm-0016")
# preprocessing.generateDF("Corpus/2gm-0017", "TF/2gm-0017")
# preprocessing.generateDF("Corpus/2gm-0018", "TF/2gm-0018")
# preprocessing.generateDF("Corpus/2gm-0019", "TF/2gm-0019")
# preprocessing.generateDF("Corpus/2gm-0020", "TF/2gm-0020")
# preprocessing.generateDF("Corpus/2gm-0021", "TF/2gm-0021")
# preprocessing.generateDF("Corpus/2gm-0022", "TF/2gm-0022")
# preprocessing.generateDF("Corpus/2gm-0023", "TF/2gm-0023")
# preprocessing.generateDF("Corpus/2gm-0024", "TF/2gm-0024")
# preprocessing.generateDF("Corpus/2gm-0025", "TF/2gm-0025")
# preprocessing.generateDF("Corpus/2gm-0026", "TF/2gm-0026")
# preprocessing.generateDF("Corpus/2gm-0027", "TF/2gm-0027")
# preprocessing.generateDF("Corpus/2gm-0028", "TF/2gm-0028")
# preprocessing.generateDF("Corpus/2gm-0029", "TF/2gm-0029")
# preprocessing.generateDF("Corpus/2gm-0030", "TF/2gm-0030")
# preprocessing.generateDF("Corpus/2gm-0031", "TF/2gm-0031")
# preprocessing.saveMetaTF()

""" codigo para generar diccionario IDF """
# preprocessing.readMetaTF()
# preprocessing.generateDictionaryIDF("Corpus/2gm-0006")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0007")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0008")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0009")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0010")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0011")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0012")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0013")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0014")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0015")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0016")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0017")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0018")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0019")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0020")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0021")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0022")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0023")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0024")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0025")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0026")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0027")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0028")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0029")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0030")
# preprocessing.generateDictionaryIDF("Corpus/2gm-0031")
# preprocessing.saveDictionaryIDF()

""" generar tabla TFIDF """
# preprocessing.loadDicionaryIDF()
# preprocessing.generateTFIDF("TF/2gm-0006", "TFIDF/2gm-0006")
# preprocessing.generateTFIDF("TF/2gm-0007", "TFIDF/2gm-0007")
# preprocessing.generateTFIDF("TF/2gm-0008", "TFIDF/2gm-0008")
# preprocessing.generateTFIDF("TF/2gm-0009", "TFIDF/2gm-0009")
# preprocessing.generateTFIDF("TF/2gm-0010", "TFIDF/2gm-0010")
# preprocessing.generateTFIDF("TF/2gm-0011", "TFIDF/2gm-0011")
# preprocessing.generateTFIDF("TF/2gm-0012", "TFIDF/2gm-0012")
# preprocessing.generateTFIDF("TF/2gm-0013", "TFIDF/2gm-0013")
# preprocessing.generateTFIDF("TF/2gm-0014", "TFIDF/2gm-0014")
# preprocessing.generateTFIDF("TF/2gm-0015", "TFIDF/2gm-0015")
# preprocessing.generateTFIDF("TF/2gm-0016", "TFIDF/2gm-0016")
# preprocessing.generateTFIDF("TF/2gm-0017", "TFIDF/2gm-0017")
# preprocessing.generateTFIDF("TF/2gm-0018", "TFIDF/2gm-0018")
# preprocessing.generateTFIDF("TF/2gm-0019", "TFIDF/2gm-0019")
# preprocessing.generateTFIDF("TF/2gm-0020", "TFIDF/2gm-0020")
# preprocessing.generateTFIDF("TF/2gm-0021", "TFIDF/2gm-0021")
# preprocessing.generateTFIDF("TF/2gm-0022", "TFIDF/2gm-0022")
# preprocessing.generateTFIDF("TF/2gm-0023", "TFIDF/2gm-0023")
# preprocessing.generateTFIDF("TF/2gm-0024", "TFIDF/2gm-0024")
# preprocessing.generateTFIDF("TF/2gm-0025", "TFIDF/2gm-0025")
# preprocessing.generateTFIDF("TF/2gm-0026", "TFIDF/2gm-0026")
# preprocessing.generateTFIDF("TF/2gm-0027", "TFIDF/2gm-0027")
# preprocessing.generateTFIDF("TF/2gm-0028", "TFIDF/2gm-0028")
# preprocessing.generateTFIDF("TF/2gm-0029", "TFIDF/2gm-0029")
# preprocessing.generateTFIDF("TF/2gm-0030", "TFIDF/2gm-0030")
# preprocessing.generateTFIDF("TF/2gm-0031", "TFIDF/2gm-0031")


""" buscador """
vector = []
pesos = []
preprocessing.findInAll("god", vector, pesos)
if (len(vector) > 0):
    print("ENCONTRADO !!!")
    # index = 0
    # while(index < len(vector)):
    #     print(vector[index] + " " + str(pesos[index]))
    #     index += 1

    vectorRes = []
    similarRes = []
    preprocessing.getSimilarInAll(vector, pesos, vectorRes, similarRes)
    index = 0
    while(index < len(vectorRes)):
        stringValue = "{:.2%}".format(similarRes[index])
        print(vectorRes[index] + " " + stringValue)
        index += 1
else:
    print("NO ENCONTRADO -_-")

