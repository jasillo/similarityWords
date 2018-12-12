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
import time

class Preprocessing:
    dictEnglish = enchant.Dict("en_US")
    stopWords = stopwords.words('english')
    stemmer = PorterStemmer()
    metaTF = []
    totalDocsTF = 0
    dictionaryIDF = {}
    umbral = 0.5
    
    maxtfidf = 0.0
    mintfidf = 100000.0
    total = 0
    sumatoria = 0

    files = ["2gm-0006", "2gm-0007", "2gm-0008","2gm-0009", "2gm-0010","2gm-0011",
        "2gm-0012", "2gm-0013", "2gm-0014", "2gm-0015", "2gm-0016", "2gm-0017", "2gm-0018",
        "2gm-0019", "2gm-0020", "2gm-0021", "2gm-0022", "2gm-0023", "2gm-0024", "2gm-0025",
        "2gm-0026", "2gm-0027", "2gm-0028", "2gm-0029", "2gm-0030", "2gm-0031"]

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
        return self.dictEnglish.check(word)

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
        if (not self.isEnglishEnchant(word)):
            return False
        if (not self.isEnglishWordNet(word)):
            return False
        if (self.isStopWord(word)):
            return False
        return True
        
    def cleanData(self):
        for my_file in self.files:
            self.cleanDataFile(my_file)

    def cleanDataFile(self, file):
        print(file)
        try:  
            fpIn = open("data/CorpusGoogle/" + file, "r")
            fpOut = open("data/CorpusClean/" + file, "w")
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

    def generateTF(self):
        for my_file in self.files:
            self.generateDF(my_file)
        self.saveMetaTF()

    def generateDF(self, file):
        print(file)
        max = 1
        prevWord = '' #docID
        features = []
        freq = []
        toralDocs = 0
        try: 
            fpIn = open("data/CorpusClean/" + file, "r")
            fpOut = open("data/TF/" + file, "w")
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
            self.metaTF.append("data/TF/" + file + " " + str(toralDocs))
        finally:
            fpIn.close()
            fpOut.close()

    def saveMetaTF(self):
        try:
            meta = open("data/TF/meta", "w")
            for item in self.metaTF:
                meta.write(item + "\n")
        finally:
            meta.close()

    def readMetaTF(self):
        try:
            meta = open("data/TF/meta", "r")
            line = meta.readline()
            words = line.split()
            while(line):
                if (len(words) >= 1):
                    tam = int(words[1])
                    self.totalDocsTF += tam 
                line = meta.readline()
                words = line.split()
            # print(self.totalDocsTF)
        finally:
            meta.close

    def generateDictionaryIDF(self):
        self.readMetaTF()
        for my_file in self.files:
            self.generateDictionaryIDFFile(my_file)
        self.saveDictionaryIDF()

    def generateDictionaryIDFFile(self, file):
        print(file)
        try:
            fpIn = open("data/CorpusClean/" + file, "r")
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
            fpOut = open("data/IDF/dictionary", "w")
            for key, value in self.dictionaryIDF.items():
                idf = math.log2(self.totalDocsTF / value)
                fpOut.write(key + " " + str(idf) + "\n")
        finally:
            fpOut.close()

    def loadDicionaryIDF(self):
        try:
            fpIn = open("data/IDF/dictionary", "r")
            line = fpIn.readline()
            words = line.split()
            while(line):
                self.dictionaryIDF[words[0]] = float(words[1])
                line = fpIn.readline()
                words = line.split()
        finally:
            fpIn.close()

    def generateTFIDF(self): 
        self.loadDicionaryIDF()
        for my_file in self.files:
            self.generateTFIDFFile(my_file)

    def generateTFIDFFile(self, file):
        print(file)
        try:
            fpIn = open("data/TF/" + file, "r")
            fpOut = open("data/TFIDF/" + file, "w")
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
            res = self.find(word, "data/TFIDF/" + item, vector, weigth)
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
            preprocessing.calculateSimilitude("data/TFIDF/" + item, vector, pesos, vectorRes, similarRes)

    def createData(self, type):
        if (type == "firts_word"):
            for my_file in self.files:
                self.dataFirtsNode(my_file)
        if (type == "second_word"):
            self.dataSecondNode()
        if (type == "link"):
            for my_file in self.files:
                self.dataLinksNode(my_file)


    def dataFirtsNode(self, file):
        print(file)
        try:
            fpIn = open("data/TFIDF/" + file, "r")
            fpOut = open("data/node_one/" + file + ".csv", "w")
            fpOut.write("name,tfidf\n")
            head = fpIn.readline()
            headSplit = head.split()
            while(head):
                tam = int(headSplit[1])
                weight = 0.0
                while(tam > 0):
                    line = fpIn.readline()
                    words = line.split()
                    if (float(words[1]) >= 0.1):
                        weight += float(words[1]) * float(words[1])
                    tam -= 1
                fpOut.write(headSplit[0] + "," + str(math.sqrt(weight)) + "\n")
                head = fpIn.readline()
                headSplit = head.split()
        except:
            print("error en " + file)
        finally:
            fpIn.close()
            fpOut.close()

    def dataSecondNode(self):
        try:
            fpIn = open("data/IDF/dictionary", "r")
            fpOut = open("data/node_two/seconds.csv", "w")
            fpOut.write("name\n")
            head = fpIn.readline()
            headSplit = head.split()
            while(head):
                fpOut.write(headSplit[0] + "\n")
                head = fpIn.readline()
                headSplit = head.split()
        except expression as identifier:
            print('error en ' + file)
        finally:
            fpIn.close()
            fpOut.close()

    def dataLinksNode(self, file):
        print(file)
        try:
            fpIn = open("data/TFIDF/" + file, "r")
            fpOut = open("data/links/" + file + ".csv", "w")
            fpOut.write("node_one,tfidf,node_two\n")
            head = fpIn.readline()
            headSplit = head.split()
            while(head):
                tam = int(headSplit[1])
                while(tam > 0):
                    line = fpIn.readline()
                    words = line.split()
                    if (float(words[1]) >= 0.1):
                        fpOut.write(headSplit[0] + "," + words[1] + "," + words[0] + "\n")
                    tam -= 1
                head = fpIn.readline()
                headSplit = head.split()
        except:
            print("error en " + file)
        finally:
            fpIn.close()
            fpOut.close()

    def statistics(self):
        for my_file in self.files:
            self.statisticsFile(my_file)
        fpOut = open("statistics", "w")
        fpOut.write("min: " + str(self.mintfidf) + "\n")
        fpOut.write("max: " + str(self.maxtfidf) + "\n")
        fpOut.write("total: " + str(self.total) + "\n")
        fpOut.write("media: " + str((self.maxtfidf + self.mintfidf) / 2.0) + "\n")
        fpOut.write("promedio: " + str(self.sumatoria / self.total) + "\n")
        fpOut.close()
            

    def statisticsFile(self, file):
        print(file)
        try:
            fpIn = open("data/TFIDF/" + file, "r")
            head = fpIn.readline()
            headSplit = head.split()
            while(head):
                tam = int(headSplit[1])
                self.total += tam
                while(tam > 0):
                    line = fpIn.readline()
                    words = line.split()
                    weight = float(words[1])
                    if (weight > self.maxtfidf):
                        self.maxtfidf = weight
                    if (weight < self.mintfidf):
                        self.mintfidf = weight
                    self.sumatoria += weight
                    tam -= 1
                head = fpIn.readline()
                headSplit = head.split()
        except:
            print("error en " + file)
        finally:
            fpIn.close()    

preprocessing = Preprocessing()

""" codigo para limpiar la data"""
# preprocessing.cleanData()

""" codigo para generar DFs """
# preprocessing.generateTF()

""" codigo para generar diccionario IDF """
# preprocessing.generateDictionaryIDF()

""" generar tabla TFIDF """
# preprocessing.generateTFIDF()


""" buscador """
start_time = time.time()
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
elapsed_time = time.time() - start_time
print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

# preprocessing.statistics()

# preprocessing.createData('firts_word')
# preprocessing.createData('second_word')
# preprocessing.createData('link')
