from neo4j import GraphDatabase
import logging
from datetime import datetime
from neo4j.exceptions import ServiceUnavailable

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_node(self, node_label, attributes):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_node, node_label, attributes)
            print(f"Node created: {result}")

    @staticmethod
    def _create_node(tx, node_label, attributes):
        query = f"CREATE (n:{node_label} {{ {', '.join([f'{k}: ${k}' for k in attributes.keys()])} }}) RETURN n"
        result = tx.run(query, **attributes)
        return result.single()[0]

    def find_node_by_attribute(self, node_label, attribute_name, attribute_value):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_node, node_label, attribute_name, attribute_value)
            for row in result:
                print(f"Found node: {row}")

    @staticmethod
    def _find_and_return_node(tx, node_label, attribute_name, attribute_value):
        query = (
            f"MATCH (n:{node_label}) "
            f"WHERE n.{attribute_name} = $attribute_value "
            "RETURN n"
        )
        result = tx.run(query, attribute_value=attribute_value)
        return [row["n"] for row in result]


    def find_relationship_by_attributes(self, label1, attr_name1, attr_value1, label2, attr_name2, attr_value2, relationship_type):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_relationship, label1, attr_name1, attr_value1, label2, attr_name2, attr_value2, relationship_type)
            for row in result:
                print(f"Found relationship: {row}")

    @staticmethod
    def _find_and_return_relationship(tx, label1, attr_name1, attr_value1, label2, attr_name2, attr_value2, relationship_type):
        query = (
            f"MATCH (n1:{label1}{{{attr_name1}: $attr_value1}})-[r:{relationship_type}]-"
            f"(n2:{label2}{{{attr_name2}: $attr_value2}}) RETURN r"
        )
        result = tx.run(query, attr_value1=attr_value1, attr_value2=attr_value2)
        return [row["r"] for row in result]


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://b5ae0841.databases.neo4j.io "
    user = "neo4j"
    password = "L7lYs5A1-8jg-tDnY6Fs52C3jAfz42CTHaeJvfGSgOc"
    app = App(uri, user, password)

    # Añadir personas con sus atributos
    #app.create_node('Person', {"name": "Harmony Korine", "tmbdld": 3, "born": datetime.strptime("1973-6-4", "%Y-%m-%d"), "die": None, "bornid": "", "url": "https://mubi.com/es/cast/harmony-korine", "imdbld": 4, "bio": "director y guionista de cine", "poster": "Harmony"})
   
    # Añadir pelicula con sus atributos
    #app.create_node('Movie', {"title": "Spider-Man: Far From Home", "tmbdld": 429617, "released": datetime.strptime("2019-07-02", "%Y-%m-%d"), "imbdRating": 7.5, "movieId": 2, "year": 2019, "imdbld": 5, "runtime": 129, "countries":["Estados Unidos"],"imbdVotes": 324,"url": "https://www.rottentomatoes.com/m/spider_man_far_from_home","revenue": 1131000000,"plot": "Peter Parker y sus amigos viajan a Europa en un viaje de estudios, pero las vacaciones toman un giro inesperado cuando Spider-Man debe enfrentar nuevas amenazas en un mundo que ha cambiado para siempre.","poster": "farfromhome","budget":160000000,"languages": [ "inglés","italiano","alemán","holandés","checo"]})
    
    #Añadir genero con su atributo
    # app.create_node('Genre', {"name": "Drama"})
    # app.create_node('Genre', {"name": "Crime"})
    # app.create_node('Genre', {"name": "Comedy"})
    # app.create_node('Genre', {"name": "Action"})
    # app.create_node('Genre', {"name": "Adventure"})
    # app.create_node('Genre', {"name": "Fantasy"})

    #Añadir usuario con sus atributos
    app.create_node('User', {"name": "Maria Santos", "userId": 1})
    # app.create_node('User', {"name": "Chris Carter", "userId": 2})
    # app.create_node('User', {"name": "Marie Guerra", "userId": 3})
    # app.create_node('User', {"name": "Robert Green", "userId": 4})
    # app.create_node('User', {"name": "Sarah Johnson", "userId": 5})

    # app.find_node_by_attribute('Person', 'name', 'John Doe')
    # app.find_rate('David','Corrupcion')
    #app.find_relationship_by_attributes("Person", "name", "Alice", "Person", "name", "David", "KNOWS")
    app.close()