import json
import mysql.connector
from decimal import Decimal


# Função que vai buscar as configurações ao config.json
def load_config():
    with open('config.json', 'r') as file:  # apenas no modo de leitura
        return json.load(file)

config = load_config()  # agora sim, chama a função para carregar a configuração

# Configurações mySQL da nuvem e do PC2
cloud_db = config["mysql_cloud"]  # dados da bd na nuvem
pc2_db = config["mysql_pc2"]  # dados da bd no PC2
tables = config["tables"]  # dicionário das tabelas e das colunas a copiar

# Estabelece a conexão com o mySQL na nuvem
try:
    cloud_conn = mysql.connector.connect(**cloud_db)  # estabelece a conexão com a bd
    cloud_cursor = cloud_conn.cursor()  # Cria um cursor para executar comandos SQL
    print("Conexão com a base de dados na nuvem estabelecida com sucesso")
except mysql.connector.Error as err:
    print(f"Erro durante conexão à nuvem: {err}")
    exit(1)  # sai do programa com erro

# Estabelece a conexão com o mySQL no PC2
try:
    pc2_db["ssl_disabled"] = True #SSL desabilitado para testes
    pc2_conn = mysql.connector.connect(**pc2_db)  # conecta ao mySQL no PC2
    pc2_cursor = pc2_conn.cursor()
    print("Conexão com a base de dados no PC2 estabelecida com sucesso")
except mysql.connector.Error as err:
    print(f"Erro durante conexão ao PC2: {err}")  # Mensagem de erro se a conexão falhar
    cloud_conn.close()  # fecha a conexão com a nuvem (se o PC2 falhar)
    exit(1)

# Inicia o processo de cópia das tabelas
for table, details in tables.items():  # percorre todas as tabelas especificadas no JSON
    columns = details["columns"]  # obtém o dicionário (com os nomes das colunas e respetivos types)
    col_names = ", ".join(columns.keys())  # converte os nomes das colunas numa string separada por vírgulas
    col_definitions = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())  # concatena nome e tipo das colunas

    # Obtém os dados da tabela na nuvem
    cloud_cursor.execute(f"SELECT {col_names} FROM {table}")  # executa um SELECT para buscar os dados na nuvem
    rows = cloud_cursor.fetchall()  # armazena os resultados da consulta

    # Cria a tabela no PC2 se ela ainda não existir
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        {col_definitions}
    )
    """
    pc2_cursor.execute(create_table_sql)  # executa a query para criar a tabela

    # Se a tabela no PC2 já existir, antes de inserir os dados no PC2, limpamos as tabelas e só mais tarde vamos escrever

    # Query para inserir os dados no PC2
    placeholders = ", ".join(["%s"] * len(columns))  # cria os placeholders para os valores (%s, %s, %s, ...)
    insert_sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"  # Query de inserção

    # Insere os dados obtidos (na bd da nuvem) na base de dados do PC2
    for row in rows:
        try:
            pc2_cursor.execute(insert_sql, row)  # insere cada linha na tabela do PC2
        except mysql.connector.Error as err:
            print(f"Erro ao inserir dados na tabela {table}: {err}")

    print(f"Tabela '{table}' transferida ({len(rows)} com sucesso)")





# Commit e fechar conexões
pc2_conn.commit()  # confirma as inserções no PC2
cloud_cursor.close()  # fecha o cursor da nuvem
cloud_conn.close()  # encerra a conexão com a nuvem
pc2_cursor.close()  # fecha o cursor do PC2
pc2_conn.close()  # encerra a conexão com o PC2

print("Transferência de todas as tabelas concluída com sucesso!")