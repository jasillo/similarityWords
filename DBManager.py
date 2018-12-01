from neo4j.v1 import GraphDatabase
import json
import math

class DataBaseManageer(object):
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "123456"

    def __init__(self):
        print("data base linked\n")
        self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def __del__(self):
        print("data base closed\n")
        self._driver.close()

    # @typeNode: tipo del nodo, "firts_word" o "second_word"
    # @nameNode: nombre del nodo, la palabra
    def createNode(self, typeNode, nameNode, tfidf = 0):
        if (typeNode == "firts_word"):
            with self._driver.session() as session:
                response = session.write_transaction(self._create_firts_word, nameNode, tfidf)
                print(response)

        if (typeNode == "second_word"):
            with self._driver.session() as session:
                response = session.write_transaction(self._create_second_word, nameNode)
                print(response)
    
    # @word: campo name del nodo q se busca
    # @return: lista de nodos adyacentes, caracteristicas
    def findNodes(self, typeNode, word):
        if (typeNode == "firts_word"):
            with self._driver.session() as session:
                response = (session.read_transaction(self._match_second_word, word))
                return response
        if (typeNode == "second_word"):
            with self._driver.session() as session:
                response = (session.read_transaction(self._match_firts_word, word))
                return response

    # @word_1: primera palabra
    # @word_2: segunda palabra, caracteristica
    # @tf: tf de la palabra 1 con respecto a la palabra 2
    def createLink(self, firtsWord, secondWord, tfidf):
        with self._driver.session() as session:
            response = session.write_transaction(self._create_link, firtsWord, secondWord, tfidf)
            print(response)

    # @word: la palabra de la cual se busca el tfidf ponderado
    # @return: tfidf de la palabra
    def getTfidf(self, word):
        with self._driver.session() as session:
            response = session.read_transaction(self._match_tfidf, word)
            return response

    def similarity(self, word):
        wordTfidf = self.getTfidf(word)
        wordLinks = self.findNodes("firts_word", word)
        arrayWords = []
        for item in wordLinks:
            print(item)
            print(self.explicitSimilarity(item["name"], wordLinks))

    # @word: palabra buscada
    # @links: filtro de los links a devolver
    def explicitSimilarity(self, word, links):
        wordLinks = self.findNodes("second_word", word)
        result = []
        for item in wordLinks:
            anotherWord = item["name"]
            if anotherWord == word:
                continue
            anotherWordTfidf = self.getTfidf(item["name"])
            anotherWordLinks = self.findNodes("firts_word", anotherWord)
            print(anotherWordLinks)
    

        x = 0
        y = 0
        result = 0
        # while( x < len(links) and y < len(wordLinks) ):
        #     if (links[x]["name"] == wordLinks[y]["name"]):
        #         result += links[x]["tfidf"] * wordLinks[y]["tfidf"]
        #         x += 1
        #         y += 1
        #     elif (vector1[x] < vector2[y]):
        #         x += 1
        #     else:
        #         y += 1
        return math.sqrt(result)

    @staticmethod
    def _create_firts_word(tx, nameNode, tfidf):
        try:
            tx.run("MERGE (a:firts_word{name: $name})"
                   "ON CREATE SET a.tfidf = $tfidf "
                   "ON MATCH SET a.tfidf = a.tfidf",
                   name=nameNode, tfidf=tfidf)
            return nameNode + " creado"
        except:
            return "erro creando : " + nameNode
        
    
    @staticmethod
    def _create_second_word(tx, nameNode):
        try:
            tx.run("MERGE (a:second_word{name: $name})",
                   name=nameNode)
            return nameNode + " creado"
        except:
            return "error creando : " + nameNode

    @staticmethod
    def _create_link(tx, firtsName, secondName, tfidf):
        try:
            tx.run("MATCH (a:firts_word{name: $firts_name}),(b:second_word{name: $second_name}) "
                   "MERGE (a)-[r:x]->(b) "
                   "ON CREATE SET r.tfidf = $tfidf "
                   "ON MATCH SET r.tfidf = r.tfidf",
                   firts_name=firtsName, second_name=secondName, tfidf=tfidf)
            return "link creado : " + firtsName + secondName
        except:
            return "error link : " + firtsName + secondName

    @staticmethod
    def _match_second_word(tx, firtsWord):
        try:
            result = tx.run("MATCH (a:firts_word{name: $name})"
                            "MATCH p=(a)-[r:x]->(b)"
                            "RETURN b.name, r.tfidf ORDER BY b.name", name=firtsWord)
            finalResult = []
            for record in result:
                finalResult.append({"name": record["b.name"], "tfidf": record["r.tfidf"]})
            return finalResult
        except:
            print("error match : " + firtsWord)
            return []

    @staticmethod
    def _match_firts_word(tx, secondWord):
        try:
            result = tx.run("MATCH (a:second_word{name: $name})"
                            "MATCH p=(b)-[r:x]->(a)"
                            "RETURN b.name, r.tfidf ORDER BY b.name", name=secondWord)
            finalResult = []
            for record in result:
                finalResult.append({"name": record["b.name"], "tfidf": record["r.tfidf"]})            
            return finalResult
        except:
            print("error match : " + secondWord)
            return []

    @staticmethod
    def _match_tfidf(tx, word):
        try:
            result = tx.run("MATCH (a:firts_word{name: $name}) "
                            "RETURN a.tfidf", name=word)
            finalResult = 0
            for record in result:
                finalResult = record["a.tfidf"]
            return finalResult
                
        except:
            print("error rfidf")
        

db = DataBaseManageer()

# db.createNode("firts_word", "gato" , 17.320508076)
# db.createNode("firts_word", "perro" , 15.874507866)
# db.createNode("firts_word", "loro" , 13.076696831)
# db.createNode("firts_word", "canario" , 17.320508076)

# db.createNode("second_word", "camina")
# db.createNode("second_word", "vuela")
# db.createNode("second_word", "ladra")
# db.createNode("second_word", "maulla")
# db.createNode("second_word", "graznida")
# db.createNode("second_word", "pequeno")
# db.createNode("second_word", "grande")

# db.createLink("perro", "camina", 10)
# db.createLink("perro", "ladra", 10)
# db.createLink("perro", "grande", 10)
# db.createLink("gato", "camina", 10)
# db.createLink("gato", "maulla", 10)
# db.createLink("gato", "pequeno", 6)
# db.createLink("gato", "grande", 4)
# db.createLink("loro", "vuela", 8)
# db.createLink("loro", "graznida", 7)
# db.createLink("loro", "pequeno", 7)
# db.createLink("loro", "grande", 3)
# db.createLink("canario", "vuela", 10)
# db.createLink("canario", "graznida", 10)
# db.createLink("canario", "pequeno", 10)

db.similarity("perro")