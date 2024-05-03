#------------------------------------- INSTALL REQUIREMENTS -------------------------------------
# Importation de fonctions depuis le module utils pour installer les dépendances, 
# se connecter à la base de données, réinitialiser la base de données, 
# et insérer des données, ainsi que les variables globales pour le nom du topic et le port du serveur

from utils import install_requirements, connection_db, reset_db, insert_data, TOPIC_NAME, SERVER_PORT

# Appel de la fonction pour installer les packages nécessaires au projet
install_requirements()

# Importation du module json pour la désérialisation des données et KafkaConsumer pour la consommation de messages
import json
from kafka import KafkaConsumer 


# ---------------------------- KAFKA CONSUMER ----------------------------
# Initialisation du Consumer Kafka. Cette section configure et lance la consommation des messages Kafka
# Le Consumer se connecte au Topic Kafka et est chargé d'alimenter la base de données avec les données récupérées

# Connexion à la base de données SQLite et réinitialisation de la table principale 
conn = connection_db()
conn = reset_db(conn)


print('Lancement de la souscription ...')

# Configuration et création du Consumer Kafka
# Le Consumer est configuré pour se connecter au serveur Kafka spécifié et s'abonner au topic défini
# La fonction 'value_deserializer' est utilisée pour transformer les données brutes en JSON
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[SERVER_PORT], 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


# Boucle de consommation des messages reçus sur le topic Kafka
# Pour chaque message, on transforme le contenu JSON en tuple et on insère dans la base de données
for event in consumer:
    event = event.value
    event_value_tuple = tuple(event.values())
    insert_data(conn, event_value_tuple)
    conn.commit()

