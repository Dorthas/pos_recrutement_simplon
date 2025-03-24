import sqlite3
import pandas as pd
import requests
import os

# Fonction pour créer la base de données et les tables
def create_database():
    print("Création de la base de données et des tables...")
    
    # Connexion à la base de données (sera créée si elle n'existe pas)
    conn = sqlite3.connect('/data/database_sqlite.db')
    cursor = conn.cursor()
    
    #Suppression des tables si elles existent pour débugger
    #cursor.execute("DROP TABLE IF EXISTS magasins")
    #cursor.execute("DROP TABLE IF EXISTS produits")
    #cursor.execute("DROP TABLE IF EXISTS ventes")   
    
    # Création de la table magasins
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS magasins (
        ID_Magasin INTEGER PRIMARY KEY,
        Ville TEXT,
        Nombre_de_salaries INTEGER
    )
    ''')
    
    # Création de la table produits
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS produits (
        ID_Reference_Produit TEXT PRIMARY KEY,
        Nom TEXT,
        Prix REAL,
        Stock INTEGER
    )
    ''')
    
    # Création de la table ventes
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ventes (
        ID_Vente INTEGER PRIMARY KEY AUTOINCREMENT,
        Date TEXT,
        ID_Reference_Produit TEXT,
        Quantite INTEGER,
        ID_Magasin INTEGER,
        FOREIGN KEY (ID_Reference_Produit) REFERENCES produits(ID_Reference_Produit),
        FOREIGN KEY (ID_Magasin) REFERENCES magasins(ID_Magasin)
    )
    ''')
        
    conn.commit()
    conn.close()
    
    print("Base de données et tables créées avec succès!")


# Fonction pour télécharger les données
def download_data():
    print("Téléchargement des données...")
    
    # URLs des fichiers (remplacés par les vraies URLs)
    magasins_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=714623615&single=true&output=csv"
    produits_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=0&single=true&output=csv"
    ventes_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=760830694&single=true&output=csv"
    
    # Créer un répertoire pour les fichiers téléchargés s'il n'existe pas
    os.makedirs('/data/csv', exist_ok=True)
    
    # Télécharger les fichiers
    try:
        # Télécharger et enregistrer le fichier magasins
        response = requests.get(magasins_url)
        response.raise_for_status()
        with open('/data/csv/magasins.csv', 'wb') as f:
            f.write(response.content)
        
        # Télécharger et enregistrer le fichier produits
        response = requests.get(produits_url)
        response.raise_for_status()
        with open('/data/csv/produits.csv', 'wb') as f:
            f.write(response.content)
        
        # Télécharger et enregistrer le fichier ventes
        response = requests.get(ventes_url)
        response.raise_for_status()
        with open('/data/csv/ventes.csv', 'wb') as f:
            f.write(response.content)
        
        print("Données téléchargées avec succès!")
        return True
    except Exception as e:
        print(f"Erreur lors du téléchargement des données: {e}")
        return False


# Fonction pour importer les données dans la base
def import_data():
    print("Importation des données dans la base...")
    
    # Connexion à la base de données
    conn = sqlite3.connect('/data/database_sqlite.db')
    
    # Importer les magasins
    try:
        magasins_df = pd.read_csv('/data/csv/magasins.csv')
        # Renommer la colonne si nécessaire
        if 'ID Magasin' in magasins_df.columns:
            magasins_df = magasins_df.rename(columns={'ID Magasin': 'ID_Magasin'})
        if 'Nombre de salariés' in magasins_df.columns:
            magasins_df = magasins_df.rename(columns={'Nombre de salariés': 'Nombre_de_salaries'})    
        # Vérifier si des données existent déjà
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM magasins")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Pas de données, importer tout
            magasins_df.to_sql('magasins', conn, if_exists='append', index=False)
            print("Données des magasins importées avec succès!")
        else:
            print("Les données des magasins existent déjà, pas d'importation nécessaire.")
    except Exception as e:
        print(f"Erreur lors de l'importation des magasins: {e}")
    
    # Importer les produits
    try:
        produits_df = pd.read_csv('/data/csv/produits.csv')
        # Renommer la colonne si nécessaire
        if 'ID Référence produit' in produits_df.columns:
            produits_df = produits_df.rename(columns={'ID Référence produit': 'ID_Reference_Produit'})
        
        # Vérifier si des données existent déjà
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM produits")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Pas de données, importer tout
            produits_df.to_sql('produits', conn, if_exists='append', index=False)
            print("Données des produits importées avec succès!")
        else:
            print("Les données des produits existent déjà, pas d'importation nécessaire.")
    except Exception as e:
        print(f"Erreur lors de l'importation des produits: {e}")
    
    # Importer les ventes
    try:
        ventes_df = pd.read_csv('/data/csv/ventes.csv')
        # Renommer les colonnes si nécessaire
        if 'ID Référence produit' in ventes_df.columns:
            ventes_df = ventes_df.rename(columns={'ID Référence produit': 'ID_Reference_Produit'})
        if 'ID Magasin' in ventes_df.columns:
            ventes_df = ventes_df.rename(columns={'ID Magasin': 'ID_Magasin'})
        if 'Quantité' in ventes_df.columns:
            ventes_df = ventes_df.rename(columns={'Quantité': 'Quantite'})    
        
        # Vérifier les ventes existantes pour n'importer que les nouvelles
        cursor = conn.cursor()
        cursor.execute("SELECT Date, ID_Reference_Produit, Quantite, ID_Magasin FROM ventes")
        existing_sales = cursor.fetchall()
        
        # Créer un ensemble des ventes existantes pour une recherche rapide
        existing_sales_set = set()
        for sale in existing_sales:
            existing_sales_set.add((sale[0], sale[1], sale[2], sale[3]))
        
        # Filtrer les nouvelles ventes
        new_sales = []
        for _, row in ventes_df.iterrows():
            sale_tuple = (row['Date'], row['ID_Reference_Produit'], row['Quantite'], row['ID_Magasin'])
            if sale_tuple not in existing_sales_set:
                new_sales.append(row.to_dict())
        
        # S'il y a de nouvelles ventes, les importer
        if new_sales:
            new_sales_df = pd.DataFrame(new_sales)
            new_sales_df.to_sql('ventes', conn, if_exists='append', index=False)
            print(f"{len(new_sales)} nouvelles ventes importées avec succès!")
        else:
            print("Pas de nouvelles ventes à importer.")
    except Exception as e:
        print(f"Erreur lors de l'importation des ventes: {e}")
    
    conn.close()


# Fonction pour exécuter les analyses
def run_analyses():
    print("Exécution des analyses...")
    
    # Connexion à la base de données
    conn = sqlite3.connect('/data/database_sqlite.db')
    cursor = conn.cursor()
    
    
    # test verification donnees
    #cursor.execute("SELECT * FROM ventes")
    #ventes_data = cursor.fetchall()
    #print("Données dans la table ventes :")
    #for row in ventes_data:
        #print(row)   
    
    #cursor.execute("SELECT * FROM produits")
    #produits_data = cursor.fetchall()
    # print("Données dans la table produits :")
    # for row in produits_data:
    #     print(row)

    # cursor.execute("SELECT * FROM magasins")
    # magasins_data = cursor.fetchall()
    # print("Données dans la table magasins :")
    # for row in magasins_data:
    #     print(row)    
    
    
    # Lire et exécuter les requêtes SQL depuis le fichier analyse.sql
    try:
        print("Lecture du fichier analyse.sql...")
        with open('/analyse.sql', 'r', encoding='utf-8') as file:
            sql_script = file.read()
        
        # Diviser les requêtes SQL par point-virgule
        queries = sql_script.split(';')
        
        # Exécuter chaque requête individuellement
        for i, query in enumerate(queries):
            query = query.strip()  # Supprimer les espaces inutiles
            if query:  # Ignorer les requêtes vides
                
            # Afficher et exécuter la requête
                print(f"--- Requête SQL : {query}")            
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                
                # Afficher les résultats
                print("\n--- Résultats ---")
                if results:
                    print(f"Nombre de résultats : {len(results)}")
                    for row in results:
                        print(row)
                    print()
                else:
                    print("Aucun résultat pour cette requête.")
            except sqlite3.Error as e:
                print(f"Erreur lors de l'exécution de la requête SQL : {e}")
        
        print("\nToutes les analyses ont été exécutées avec succès!")
    except Exception as e:
        print(f"Erreur lors de l'exécution des analyses depuis le fichier analyse.sql: {e}")
    finally:
        # Valider et fermer la connexion
        conn.commit()
        conn.close()


# Fonction principale
def main():
    print("Démarrage du processus d'analyse des ventes...")
    
    # Créer la base de données et les tables
    create_database()
    
    # Télécharger les données
    if download_data():
        # Importer les données
        import_data()
        
        # Exécuter les analyses
        run_analyses()
    
    print("Processus terminé!")

# Point d'entrée du script
if __name__ == "__main__":
    main()