from neo4j.v1 import GraphDatabase
import json

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
    
    # @word_1: primera palabra
    # @word_2: segunda palabra, caracteristica
    # @tf: tf de la palabra 1 con respecto a la palabra 2
    def createLink(self, word_1, word_2, tf):
        print("")
        
    # @word: campo name del nodo q se busca
    # @return: lista de nodos adyacentes, caracteristicas
    def findNodes(self, typeNode, word):
        if (typeNode == "firts_word"):
            with self._driver.session() as session:
                print(session.read_transaction(self._match_second_word, word))
        if (typeNode == "second_word"):
            with self._driver.session() as session:
                print(session.read_transaction(self._match_firts_word, word))

    def createLink(self, firtsWord, secondWord, tfidf):
        with self._driver.session() as session:
            response = session.write_transaction(self._create_link, firtsWord, secondWord, tfidf)
            print(response)

    @staticmethod
    def _create_firts_word(tx, nameNode, tfidf):
        try:
            tx.run("MERGE (a:firts_word{name: $name})"
                   "ON CREATE SET a.tfidf = $tfidf "
                   "ON MATCH SET a.tfidf = a.tfidf",
                   name=nameNode, tfidf=tfidf)
            return nameNode + " creado"
        except expression as identifier:
            return "erro creando : " + nameNode
        
    
    @staticmethod
    def _create_second_word(tx, nameNode):
        try:
            tx.run("MERGE (a:second_word{name: $name})",
                   name=nameNode)
            return nameNode + " creado"
        except expression as identifier:
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
        except expression as identifier:
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
        except expression as identifier:
            print("error match : " + firtsWord)
            return []

    @staticmethod
    def _match_firts_word(tx, secondWord):
        try:
            result = tx.run("MATCH (a:second_word{name: $name})"
                            "MATCH p=(b)-[r:x]->(a)"
                            "RETURN b.name ORDER BY b.name", name=secondWord)
            finalResult = []
            for record in result:
                finalResult.append({"name": record["b.name"]})            
            return finalResult
        except expression as identifier:
            print("error match : " + secondWord)
            return []
        

db = DataBaseManageer()
# db.createNode("firts_word", "gato" , 15)
# db.createNode("firts_word", "perro" , 20)
# db.createNode("second_word", "ladra")
# db.createNode("second_word", "maulla")
# db.createNode("second_word", "come")
# db.findNodes("perro")
# db.createLink("perro", "ladra", 9)
# db.createLink("perro", "come", 9)
# db.createLink("gato", "come", 9)
# db.createLink("gato", "maulla", 6)
db.findNodes("firts_word", "gato")
# db.findNodes("second_word", "come")