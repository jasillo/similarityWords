from neo4j.v1 import GraphDatabase

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
    def createNode(self, typeNode, nameNode):
        if (typeNode == "firts_word"):
            with self._driver.session() as session:
                response = session.write_transaction(self._create_firts_word, nameNode)
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
    def findNodes(self, word):
        with self._driver.session() as session:
            print(session.read_transaction(self._match_second_word, word))

    def createLink(self, firtsWord, secondWord):
        with self._driver.session() as session:
            session.write_transaction(self._create_link, firtsWord, secondWord)

    @staticmethod
    def _create_firts_word(tx, nameNode):
        return tx.run("MERGE (a:firts_word{name: $name})"
                      "RETURN a", name=nameNode).single()
    
    @staticmethod
    def _create_second_word(tx, nameNode):
        return tx.run("MERGE (a:second_word{name: $name})"
                      "RETURN a", name=nameNode).single()

    @staticmethod
    def _create_link(tx, firtsName, secondName):
        return tx.run("MATCH (a:firts_word{name: $firts_name}),(b:second_word{name: $second_name})"
                      "MERGE (a)-[r:x]->(b)", firts_name=firtsName, second_name=secondName)

    @staticmethod
    def _match_second_word(tx, firtsWord):
        result = tx.run("MATCH (a:firts_word{name: $name})"
                        "MATCH p=(a)-[r:x]->(b)"
                        "RETURN b.name ORDER BY b.name", name=firtsWord)
        return [record["b.name"] for record in result]

db = DataBaseManageer()
# db.createNode("firts_word", "gato")
# db.createNode("second_word", "ladra")
# db.findNodes("perro")
# db.createLink("perro", "ladra")
# db.createLink("perro", "come")
# db.createLink("gato", "come")
db.findNodes("perro")