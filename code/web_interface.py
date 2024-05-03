# Importation des bibliothèques nécessaires pour le traitement et la visualisation des données
import pandas as pd
import seaborn as sns
import streamlit as st
import matplotlib.pyplot as plt
from utils import connection_db, get_data 


# La fonction permet de créer l'interface et les différentes visualisations de notre page
def viz(df):
    # Définition du titre de la page web dans l'application Streamlit
    st.title("Suivi des transactions clients")

    # Affichage d'un tableau contenant les données initiales du dataframe
    st.table(df)

    # Section pour filtrer les données par date à l'aide d'interfaces utilisateur interactives
    st.subheader("Transactions filtrées")

    # Conversion de la colonne des dates en type date pour faciliter le filtrage
    df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
    min_date = df['transaction_date'].min()
    max_date = df['transaction_date'].max()

    # Création de deux sélecteurs de dates pour définir un intervalle de temps
    date_debut = st.date_input("Date de début", value=min(df['transaction_date']), min_value=min_date, max_value=max_date)
    date_fin = st.date_input("Date de fin", value=max(df['transaction_date']), min_value=min_date, max_value=max_date)
    # Filtrage des données selon l'intervalle sélectionné
    df_filtre = df[(df["transaction_date"] >= date_debut) & (df["transaction_date"] <= date_fin)]
    # Affichage du dataframe filtré
    st.table(df_filtre)

    # Visualisation sous forme de heatmap : Nombre de transactions par compte et type
    st.subheader('Transactions par compte')
    fig1, ax1 = plt.subplots(figsize=(5, 2))
    chord_data = df.groupby(['account', 'transaction_type']).size().unstack(fill_value=0)
    sns.heatmap(chord_data, annot=True, ax=ax1)
    plt.ylabel('Compte')
    # Affichage de la figure
    st.pyplot(fig1)

    # Visualisation sous forme de barres horizontales : Montant des transactions par type et par compte
    fig2, ax2 = plt.subplots(figsize=(7, 4))
    transaction_totals = df.pivot_table(index='account', columns='transaction_type', values='transaction_value', 
                                        aggfunc='sum', fill_value=0)
    # Boucle pour créer les barres dans le diagramme en barres
    for i, transaction_type in enumerate(['deposit', 'withdrawal']):
        color = 'green' if transaction_type == 'deposit' else 'red'
        left_positions = transaction_totals['deposit'] if transaction_type == 'withdrawal' else 0
        ax2.barh(transaction_totals.index, transaction_totals[transaction_type], color=color, left=left_positions, 
                 label=transaction_type.capitalize())
    ax2.set_ylabel('Compte')
    ax2.legend(title='Type de transaction')
    # Affichage de la figure
    st.pyplot(fig2)

    # Visualisation sous forme de graphique linéaire : Évolution de la balance au fil du temps pour un compte sélectionné
    st.subheader('Évolution de la balance au fil du temps')
    fig3, ax3 = plt.subplots(figsize=(7, 4))
    # Sélection d'un compte via un menu déroulant
    account = st.selectbox("Choisir le compte:", df['account'].unique())
    df_acc = df[df['account']==account]
    plt.figure(figsize=(7, 4))
    sns.lineplot(data=df_acc, x=df_acc.index, y='balance', marker='o', ax=ax3, err_style=None)
    ax3.set_xticks([])
    # Affichage de la figure
    st.pyplot(fig3)


# La fonction établit la connexion avec la base données et permet de lancer l'application Streamlit
def launch_web_app():
    # Connexion à la base de données et gestion des erreurs de connexion
    conn = connection_db()
    if not conn:
        st.error("Failed to connect to the database.")
        return
    
    # Récupération des données de la base de données et appel de la fonction de visualisation
    df = get_data(conn)
    viz(df)
    # Fermeture de la connexion à la base de données
    conn.close()


if __name__ == "__main__":
    launch_web_app()