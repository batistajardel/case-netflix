import psycopg2
import requests

# Dados de conexão com o banco de dados do PostgreSQL
db_params = {
    "host": "xxxxxxxxxx",
    "database": "filmes",
    "user": "Jardel",
    "password": "**********"
}

# Dados da API do AMiner Gender
gender_api_url = "https://innovaapi.aminer.cn/tools/v1/predict/gender"

# Função para atualizar a tabela de fato com informações de gênero
def update_fact_table_with_gender(connection, cursor):
    select_query = """
        SELECT show_id, cast FROM Fact_Show;
    """
    cursor.execute(select_query)
    rows = cursor.fetchall()

    for row in rows:
        show_id = row[0]
        cast = row[1]

        gender_data = get_gender_data(cast)
        update_query = """
            UPDATE Fact_Show
            SET gender = %s
            WHERE show_id = %s;
        """
        cursor.execute(update_query, (gender_data, show_id))
    
    connection.commit()

# Função para obter o gênero dos membros do elenco usando a API do AMiner Gender
def get_gender_data(cast):

    # Formatação da URL de consulta para a API
    query_params = {
        "name": "+".join(cast.split()),  # Formatação de nomes para URL
        "org": ""  # Como no case não é relevante, fica fazio
    }

    # Realizando a chamada HTTP à API
    response = requests.get(gender_api_url, params=query_params)

    # Analisando o body de retorno
    if response.status_code == 200:

        # Parseando a resposta JSON
        api_data = response.json()

        # Acessando os dados de gênero do JSON da resposta
        gender = api_data.get("data", {}).get("Final", {}).get("gender", "UNKNOWN")
        return gender

    else:
        print("Erro na chamada à API:", response.text)
        return "UNKNOWN"


if __name__ == "__main__":
    try:
        # Conectando ao banco de dados
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Atualizando a tabela fato com informações de gênero
        update_fact_table_with_gender(connection, cursor)

    except psycopg2.Error as e:
        print("Erro ao interagir com o banco de dados:", e)

    finally:
        if connection:
            connection.close()
