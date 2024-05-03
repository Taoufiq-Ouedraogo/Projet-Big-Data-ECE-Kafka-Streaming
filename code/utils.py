#------------------------------------- INSTALL REQUIREMENTS -------------------------------------

# Importation des modules nécessaires pour l'exécution des fonction
import subprocess, sys, os


# La fonction sera appelé lors de l'exécution du Consumer
def install_requirements():
    # Liste des packages nécessaires pour le projet
    packages = ["kafka-python==2.0.2", "sqlite3", "pandas", "datetime", "csv", "streamlit"]

    # Boucle pour traiter chaque package dans la liste
    for pckg in packages:
        try:
            # Vérifier si le paquet est déjà installé en utilisant pip show
            subprocess.run(["pip", "show", pckg], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(f"{pckg} est déjà installé.")
        except:
            # Bloc pour gérer le cas où un paquet n'est pas trouvé et doit être installé
            try:
                # Installation du paquet manquant via pip install
                subprocess.run(["pip", "install", pckg], check=True)
                print(f"{pckg} installé avec succès.")
            except Exception as e:
                # Gestion des erreurs d'installation
                print(f"Erreur lors de l'installation de {pckg} :", e)
    

#------------------------------------- VARIABLES -------------------------------------

# Définition des variables globales utilisées dans le projet
SERVER_PORT = "localhost:9092"
TOPIC_NAME = 'ProjectBigData'


#------------------------------------- BDD -------------------------------------
# Section dédiée à la gestion de la base de données SQLite


# La fonction permet d'établir la connexion avec la base de données SQLite
# elle sera appelé lors de l'exécution du Consumer et du Producer
def connection_db():
    import sqlite3
    conn = None
    try:
        # Tentative de connexion à la base de données SQLite
        conn = sqlite3.connect('./data/TransactionDB.db')
        print("Connection to SQLite DB successful")
    except sqlite3.Error as e:
        # Gestion des erreurs de connexion
        print(f"The error '{e}' occurred")

    return conn


# La fonction permet de réinitialiser la base de données
def reset_db(conn):
    import sqlite3
    cursor = conn.cursor()

    try:
         # Suppression de la table transactions si elle existe déjà
        cursor.execute("DROP TABLE IF EXISTS transactions")
      
        # Création de la table transactions avec les champs nécessaires
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_date DATETIME,
            account TEXT,
            transaction_value NUMERIC(10,2),
            balance NUMERIC(10,2),
            transaction_type TEXT
        )
        ''')
        conn.commit()  
        print("Creation of transactions Table successful")

    except sqlite3.Error as e:
        # Gestion des erreurs lors de la création de la table
        print(f"An error occurred: {e}")
        conn.rollback() 
        
    finally:
        # Fermeture du curseur utilisé pour exécuter les requêtes SQL
        if cursor:
            cursor.close()

    return conn


# La fonction permet d'insérer de nouveaux éléments dans la base de données
# Le Consumer aura cette tâche
def insert_data(conn, data):
    import sqlite3, csv, datetime
    cursor = conn.cursor()
    
    # Préparation de la requête SQL pour insérer des données dans la table transactions
    query = "INSERT INTO transactions (transaction_date, account, transaction_value, balance, transaction_type) VALUES (?, ?, ?, ?, ?)"
    try:
        # Exécution de la requête d'insertion avec les données fournies
        cursor.execute(query, data)
        conn.commit()
        print("Data inserted successfully")

    except sqlite3.Error as e:
        # Gestion des erreurs d'insertion
        print(f"The error '{e}' occurred")
    finally:
        # Fermeture du curseur
        cursor.close()
        

# La fonction permet de récupérer toutes les données de la base de données
# Elle sera utilisée par le Producer pour permettre l'affichage dans nos interfaces web et Tkinter
def get_data(conn):
    import pandas as pd
    import sqlite3
    cursor = conn.cursor()

    # Préparation de la requête SQL pour récupérer toutes les transactions
    query = "SELECT * FROM transactions"
    try:
        # Exécution de la requête et récupération des résultats
        cursor.execute(query)
        result = cursor.fetchall()

        # Extraction des noms de colonnes à partir du curseur pour les utiliser dans un DataFrame
        column_names = [description[0] for description in cursor.description]
        data = [dict(zip(column_names, row)) for row in result]
        return pd.DataFrame(data)
    
    except sqlite3.Error as e:
        # Gestion des erreurs lors de la récupération des données
        print(f"The error '{e}' occurred")
        return pd.DataFrame()
    