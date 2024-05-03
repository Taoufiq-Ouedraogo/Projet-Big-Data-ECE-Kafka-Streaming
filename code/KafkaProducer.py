# ---------------------------- KAFKA PRODUCER ----------------------------
# Importation des bibliothèques nécessaires pour la création du producteur Kafka,
# la manipulation des données et l'envoi des enregistrements dans le topic Kafka

import pandas as pd
import tkinter as tk
import json, time, datetime, subprocess
from kafka import KafkaProducer
from utils import connection_db, get_data, TOPIC_NAME, SERVER_PORT


# ---------------------------- TRAITEMENTS DONNEES ----------------------------
# 1. Ici, nous allons **créer une nouvelle colonne** `transaction_type`:
#    - les valeurs de transactions positives sont des `dépôts` 
#    - les transactions négatives sont des `retraits`
# 2. Ensuite, nous allons **transformer la colonne** `transaction_value` pour avoir 
#    des valeurs positives.
# 3. Puis, nous allons **formatter la colonne** `transaction_date` pour avoir un format 
#    manipulable par les bases de données.


# Lecture des données à partir du fichier CSV contenant les transactions des clients
df = pd.read_csv("./data/client_transactions.csv", sep=';')

# Création d'une nouvelle colonne 'transaction_type' pour identifier les dépôts et les retraits
df['transaction_type']=df['transaction_value'].apply(lambda x: 'deposit' if x>0 else 'withdrawal')

# Transformation des valeurs négatives de 'transaction_value' en valeurs positives
df['transaction_value'] = df['transaction_value'].abs()

# Formatage de la colonne 'transaction_date' pour qu'elle soit manipulable par les bases de données
df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%d/%m/%Y')
df['transaction_date'] = df['transaction_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['transaction_date'] = df['transaction_date'].astype(str)


# Envoie des données dans le Topic
def send_records(df, producer):
    for i in range(len(df)):
        event = df.iloc[i, :].to_dict()
        producer.send(TOPIC_NAME, value=event)
        # Pause d'une seconde entre l'envoi de chaque enregistrement
        time.sleep(1)
    print('Envoie des données au Topic')


# Creation du Consomer Kafka 
# Spécification de l'adresse du serveur Kafka
# Sérialisation des valeurs des enregistrements en JSON
producer = KafkaProducer(
    bootstrap_servers=[SERVER_PORT],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


# ---------------------------- INTERFACE GRAPHIQUE ----------------------------
# Création de l'interface graphique Tkinter

class Application(tk.Tk):
    def __init__(self, conn, producer):
        # Méthode spéciale appelée lors de la création d'une instance de la classe
        super().__init__()

        # Initialisation des variables membres de la classe pour la gestion des données
        self.account = None
        self.amount = None
        self.transaction = None
        self.date = None
        self.balance = None
        
        # Connexion à la base de données et récupération des données initiales
        self.conn = conn
        self.df = get_data(self.conn)

        # Création de la première fenêtre de l'application
        self.create_first_window()

    # Méthode pour effacer tous les widgets de la fenêtre
    def clear_window(self):
        # Supprimer les objets de la fenêtre
        for widget in self.winfo_children():
            widget.destroy()

    # Méthode pour centrer la fenêtre sur l'écran 
    def center_window(self):
            self.update_idletasks()
            w = self.winfo_width()
            h = self.winfo_height()
            x = (self.winfo_screenwidth() // 2) - (w // 2)
            y = (self.winfo_screenheight() // 2) - (h // 2)
            self.geometry(f"{w}x{h}+{x}+{y}")

    # Méthode pour valider le montant de la transaction 
    def validate_amount_transac(self, amount):
        try:
            if float(amount)>0:
                return True
        except ValueError:
            return False
    
    # Méthode pour mettre à jour la base de données avec les nouvelles données de transaction
    def update_bdd(self):        
        new_df = pd.DataFrame(columns=self.df.columns)
        new_df.loc[len(new_df)] = [self.date, self.account, self.amount, self.balance, self.transaction]
        send_records(new_df, producer)
        
        #conn = connection_db()
        cursor = conn.cursor()
        conn.commit()
        self.conn = conn
        self.df = get_data(self.conn)
        
    
    # Méthode pour créer la première fenêtre de l'application
    def create_first_window(self):
        self.df = get_data(self.conn)
        
        self.clear_window()
        self.title("Account Connection")
        self.geometry("400x150")
        self.center_window()
        
        # Label et zone de saisie pour entrer le numéro de compte
        self.label = tk.Label(self, text="Enter Account Number :")
        self.label.pack(pady=10)
        
        self.account_entry = tk.Entry(self)
        self.account_entry.pack()
        self.account_entry.focus_set()

        # Bouton pour valider la connexion
        self.submit_button = tk.Button(self, text="Connection", command=self.connection)
        self.submit_button.pack()
        
        # Affichage des numéros de compte disponibles
        txt = f"Account Number Available : \n{list(self.df['account'].unique())}"
        self.account_accepted = tk.Label(self, text=txt)
        self.account_accepted.pack(pady=7)


    # Méthode pour créer la deuxième fenêtre de l'application (gestion des transactions)
    def create_second_window(self):
        self.df = get_data(self.conn)
        
        self.clear_window()
        self.title("Make Transaction")
        self.larg = 400
        self.long = 190
        self.geometry(f"{self.larg}x{self.long}")
        self.center_window()
        
        self.balance = float(self.df[self.df['account']==self.account]['balance'].values[-1])

        # Labels pour afficher le numéro de compte et le solde actuel
        self.label2 = tk.Label(self, text=f"Account Number : \"{self.account}\"\n")
        self.label2.pack(pady=1)

        self.label3 = tk.Label(self, text=f"Current Balance -->> \"{self.balance}\"\n")
        self.label3.pack(pady=1)

        # Label et zone de saisie pour entrer le montant de la transaction
        self.amount_label = tk.Label(self, text="Enter Transaction Amount :")
        self.amount_label.pack(pady=1)

        self.amount_entry = tk.Entry(self)
        self.amount_entry.pack()
        self.amount_entry.focus_set()

        # Boutons pour effectuer des dépôts, des retraits et revenir à la première fenêtre
        self.deposit_button = tk.Button(self, text="Deposit", command=self.deposit)
        self.deposit_button.place(x=self.larg-320, y=self.long-50)

        self.withdraw_button = tk.Button(self, text="Withdrawal", command=self.withdraw)
        self.withdraw_button.place(x=self.larg-231, y=self.long-50)

        self.back_button = tk.Button(self, text="Back", command=self.create_first_window)
        self.back_button.place(x=self.larg-120, y=self.long-50)
            
    # Méthode pour gérer la connexion à un compte  
    def connection(self):
        self.account = self.account_entry.get()
        
        if self.account in self.df['account'].unique():
            self.create_second_window()
        
    # Méthode pour effectuer un dépôt
    def deposit(self):
        amount = self.amount_entry.get()
        
        if self.validate_amount_transac(amount):
            self.amount = float(amount)
            self.transaction = 'deposit'
            self.balance = self.balance + self.amount
            self.date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            mssg = f"Depositing {self.amount} into account \"{self.account}\" at {self.date} | New Balance = {self.balance}"
            print(mssg)
            self.update_bdd()
            self.create_second_window()
        
    # Méthode pour effectuer un retrait
    def withdraw(self):
        amount = self.amount_entry.get()
        
        if self.validate_amount_transac(amount):
            self.amount = float(self.amount_entry.get())
            self.transaction = 'withdrawal'
            self.balance = self.balance - self.amount
            self.date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            mssg = f"Withdrawing \"{self.amount}\" into account \"{self.account}\" at \"{self.date}\" | New Balance = \"{self.balance}\""
            print(mssg)
            self.update_bdd()
            self.create_second_window()
        

if __name__ == "__main__":
    # Connexion à la base de données
    conn = connection_db()
    # Envoie des cinq premiers enregistrements du dataframe `df` au topic Kafka à l'aide du producteur défini.
    send_records(df.iloc[:5, :], producer)
    
    # Lancement de la WebApp
    # Utilisation de subprocess.Popen pour exécuter une commande shell qui lance l'application Streamlit
    # `shell=True` permet l'exécution de la commande dans le shell. `stdout` et `stderr` sont redirigés
    print('Lancement de la web app')
    subprocess.Popen("streamlit run code/web_interface.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Création d'une instance de l'application Tkinter
    print('Lancement de l\'interface Tkinter')
    app = Application(conn, producer)
    # Démarrage de la boucle principale de Tkinter, qui attend les interactions utilisateur
    app.mainloop()