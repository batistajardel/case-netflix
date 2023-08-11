import csv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Dados de conexão com o banco de dados do PostgreSQL
db_params = {
    "host": "xxxxxxxxxx",
    "database": "filmes",
    "user": "Jardel",
    "password": "**********"
}

# Dados do arquivo CSV
csv_file = "netflix_titles.csv"

# Scripts SQL para criação das tabelas
create_dim_date = """

    CREATE TABLE Dim_Date (
        date_id SERIAL PRIMARY KEY,
        date_value DATE NOT NULL
    );

"""

create_dim_country = """

    CREATE TABLE Dim_Country (
        country_id SERIAL PRIMARY KEY,
        country_name VARCHAR(100) NOT NULL
    );

"""

create_dim_genre = """

    CREATE TABLE Dim_Genre (
        genre_id SERIAL PRIMARY KEY,
        genre_name VARCHAR(100) NOT NULL
    );

"""

create_fact_show = """

    CREATE TABLE Fact_Show (
        fact_id SERIAL PRIMARY KEY,
        show_id VARCHAR(50) NOT NULL,
        date_id INT REFERENCES Dim_Date(date_id),
        country_id INT REFERENCES Dim_Country(country_id),
        genre_id INT REFERENCES Dim_Genre(genre_id),
        type VARCHAR(10),
        title VARCHAR(255),
        director VARCHAR(100),
        cast VARCHAR(255),
        release_year INT,
        rating VARCHAR(10),
        duration VARCHAR(50),
        description TEXT
    );

"""

# Função para criar tabelas
def create_tables(connection, cursor):
    cursor.execute(create_dim_date)
    cursor.execute(create_dim_country)
    cursor.execute(create_dim_genre)
    cursor.execute(create_fact_show)
    connection.commit()

# Função para realizar a inserção na tabela de dimensão
def insert_into_dimension_table(connection, cursor, table_name, column_name, values):
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s;").format(
        sql.Identifier(table_name),
        sql.Identifier(column_name)
    )
    execute_values(cursor, insert_query, [(value,) for value in values])
    connection.commit()

# Função para realizar o processo de ETL nas tabelas de dimensão
def etl_dimension_tables(connection, cursor, csv_file):
    with open(csv_file, "r", encoding="utf-8") as file:
        csv_reader = csv.DictReader(file)

        distinct_dates = set()
        distinct_countries = set()
        distinct_genres = set()

        for row in csv_reader:
            distinct_dates.add(row["Date_added"])
            distinct_countries.add(row["Country"])
            distinct_genres.update(row["Listed_in"].split(", "))

        # Inserção nas tabelas de dimensão
        insert_into_dimension_table(connection, cursor, "Dim_Date", "date_value", distinct_dates)
        insert_into_dimension_table(connection, cursor, "Dim_Country", "country_name", distinct_countries)
        insert_into_dimension_table(connection, cursor, "Dim_Genre", "genre_name", distinct_genres)

# Função para realizar o processo de ETL na tabela de fato
def etl_fact_table(connection, cursor, csv_file):
    with open(csv_file, "r", encoding="utf-8") as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            
            insert_query = """
                INSERT INTO Fact_Show (
                    show_id,
                    date_id,
                    country_id,
                    genre_id,
                    type,
                    title,
                    director,
                    cast,
                    release_year,
                    rating,
                    duration,
                    description
                ) VALUES (
                    %(show_id)s,
                    (SELECT date_id FROM Dim_Date WHERE date_value = %(Date_added)s),
                    (SELECT country_id FROM Dim_Country WHERE country_name = %(Country)s),
                    (SELECT genre_id FROM Dim_Genre WHERE genre_name = %(Listed_in)s),
                    %(type)s,
                    %(Title)s,
                    %(Director)s,
                    %(Cast)s,
                    %(Release_year)s,
                    %(Rating)s,
                    %(Duration)s,
                    %(description)s
                );
            """
            execute_values(cursor, insert_query, [row])
        
        connection.commit()


def main():
    try:
        # Conectando ao banco de dados
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Criando as tabelas
        create_tables(connection, cursor)

        # Realizando o ETL nas tabelas de dimensão
        etl_dimension_tables(connection, cursor, csv_file)

        # Realizando o ETL na tabela de fato
        etl_fact_table(connection, cursor, csv_file)

    except psycopg2.Error as e:
        print("Erro ao interagir com o banco de dados:", e)

    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
