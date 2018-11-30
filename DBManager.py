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

    # @type: tipo del nodo, "firts_word" o "second_word"
    # @name: nombre del nodo, la palabra
    def createNode(self, type, name):
    
    # @word_1: primera palabra
    # @word_2: segunda palabra, caracteristica
    # @tf: tf de la palabra 1 con respecto a la palabra 2
    def createLink(self, word_1, word_2, tf):

    # @word: campo name del nodo q se busca
    # @return: lista de nodos adyacentes, caracteristicas
    def findNode(self, word):

    def print_greeting(self, message):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]