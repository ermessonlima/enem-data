import threading
import queue
import psycopg2
from psycopg2 import extras
import pandas as pd 
from tqdm import tqdm

# Variável global para controle de cancelamento
cancelar = False

# Função para criar a tabela escola
def create_table_escola(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS escola (
        CO_MUNICIPIO_ESC BIGINT PRIMARY KEY,
        SG_UF_ESC VARCHAR(2),
        TP_ESCOLA INTEGER
    );
    """
    cursor.execute(create_table_query)


# Função para criar a tabela participante e relacionar com escola
def create_table_participante(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS participante (
        NU_INSCRICAO BIGINT PRIMARY KEY,
        TP_FAIXA_ETARIA INTEGER,
        TP_SEXO VARCHAR(1),
        TP_COR_RACA INTEGER,
        IN_TREINEIRO INTEGER,
        CO_MUNICIPIO_ESC BIGINT REFERENCES escola(CO_MUNICIPIO_ESC),
        NU_NOTA_REDACAO DECIMAL,
        NU_NOTA_CN DECIMAL,
        NU_NOTA_CH DECIMAL,
        NU_NOTA_LC DECIMAL,
        NU_NOTA_MT DECIMAL,
        TP_PRESENCA INTEGER,
        MEDIA_GERAL DECIMAL
    );
    """
    cursor.execute(create_table_query)

# Função para controlar o cancelamento
def solicitar_cancelamento():
    global cancelar
    cancelar = True
    print("Encerramento solicitado... Finalizando o processamento atual.")

# Configuração de conexão com o banco de dados PostgreSQL
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",  
        user="postgres",
        password="postgres",
        options="-c default_transaction_isolation=serializable"
    )
    return conn

# Função para inserir os dados nas tabelas
def insert_batch(cursor, conn, data_participante, data_escola, retry_count=3):
    for attempt in range(retry_count):
        try:
            # Inserção na tabela escola
            query_escola = """
            INSERT INTO escola (CO_MUNICIPIO_ESC, SG_UF_ESC, TP_ESCOLA)
            VALUES %s
            ON CONFLICT (CO_MUNICIPIO_ESC) DO NOTHING
            """
            extras.execute_values(cursor, query_escola, data_escola)

            # Inserção na tabela participante
            query_participante = """
            INSERT INTO participante (NU_INSCRICAO, TP_FAIXA_ETARIA, TP_SEXO, TP_COR_RACA, IN_TREINEIRO, CO_MUNICIPIO_ESC, NU_NOTA_REDACAO, NU_NOTA_CN, NU_NOTA_CH, NU_NOTA_LC, NU_NOTA_MT, TP_PRESENCA, MEDIA_GERAL)
            VALUES %s
            ON CONFLICT (NU_INSCRICAO) DO NOTHING
            """
            extras.execute_values(cursor, query_participante, data_participante)

            conn.commit()
            break  # Se der tudo certo, sai do loop
        except psycopg2.errors.DeadlockDetected:
            conn.rollback()
            print(f"Deadlock detectado na tentativa {attempt + 1}. Retentando...")
            if attempt == retry_count - 1:
                raise  # Se falhar na última tentativa, levante a exceção
        except Exception as e:
            conn.rollback()
            print(f"Erro ao inserir no banco de dados: {e}")
            raise

# Função para processar os dados e colocar na fila
def process_csv_chunk(caminho_arquivo, fila, total_chunks):
    global cancelar
    with tqdm(total=total_chunks, desc="Processando CSV") as pbar:
        for i, chunk in enumerate(pd.read_csv(caminho_arquivo, sep=';', encoding='ISO-8859-1', chunksize=10000)):
            if cancelar:
                break

            # Preparar os dados para a tabela escola
            data_escola = [
                (
                    row['CO_MUNICIPIO_ESC'],
                    row['SG_UF_ESC'],
                    row['TP_ESCOLA']
                )
                for _, row in chunk.iterrows()
            ]

            # Preparar os dados para a tabela participante
            data_participante = [
                (
                    row['NU_INSCRICAO'],
                    row['TP_FAIXA_ETARIA'],
                    row['TP_SEXO'],
                    row['TP_COR_RACA'],
                    row['IN_TREINEIRO'],
                    row['CO_MUNICIPIO_ESC'],
                    row['NU_NOTA_REDACAO'],
                    row['NU_NOTA_CN'],
                    row['NU_NOTA_CH'],
                    row['NU_NOTA_LC'],
                    row['NU_NOTA_MT'],
                    row['TP_PRESENCA'],
                    row['MEDIA_GERAL']
                )
                for _, row in chunk.iterrows()
            ]

            # Coloca os dados na fila
            fila.put((data_participante, data_escola))
            print(f"Chunk {i+1} processado e enviado para a fila")
            pbar.update(1)

# Função que consome da fila e insere no banco
# Função que consome da fila e insere no banco
def inserir_dados_na_fila(fila, progress_bar):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Criação das tabelas, se não existirem
    create_table_escola(cursor)
    create_table_participante(cursor)
    conn.commit()

    while not cancelar:
        try:
            result = fila.get(timeout=5)
            if result is None:
                break
            
            # Verifica se o valor não é None antes de tentar desempacotar
            data_participante, data_escola = result
            
            insert_batch(cursor, conn, data_participante, data_escola)
            fila.task_done()
        except queue.Empty:
            if cancelar:
                break
        except Exception as e:
            print(f"Erro ao processar dados na thread: {e}")

    cursor.close()
    conn.close()


# Função para calcular o número de chunks no CSV
def calcular_total_chunks(caminho_arquivo, chunksize=10000):
    total_linhas = sum(1 for _ in open(caminho_arquivo, 'r', encoding='ISO-8859-1')) - 1  # Ignora o cabeçalho
    total_chunks = (total_linhas // chunksize) + 1
    return total_chunks

# Função principal
def main():
    global cancelar
    caminho_arquivo = 'microdados_enem_2023_l.csv'
    fila = queue.Queue(maxsize=20)

    total_chunks = calcular_total_chunks(caminho_arquivo)

    with tqdm(total=total_chunks, desc="Inserindo no Banco") as progress_bar:
        threads = []
        for i in range(5):  # 5 threads para inserção paralela
            t = threading.Thread(target=inserir_dados_na_fila, args=(fila, progress_bar,))
            t.daemon = True
            threads.append(t)
            t.start()

        try:
            process_csv_chunk(caminho_arquivo, fila, total_chunks)
            fila.join()
        except KeyboardInterrupt:
            solicitar_cancelamento()
            fila.join()

        for _ in threads:
            fila.put(None)

        for t in threads:
            t.join()

if __name__ == "__main__":
    main()
