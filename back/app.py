import threading
import queue
import psycopg2
from psycopg2 import extras
import pandas as pd
import time
from tqdm import tqdm  
 
cancelar = False

# Função para verificar se a tabela existe e, caso contrário, criá-la
def create_table_if_not_exists(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS enem_data (
        NU_INSCRICAO BIGINT PRIMARY KEY,
        NU_ANO INTEGER,
        TP_FAIXA_ETARIA INTEGER,
        TP_SEXO VARCHAR(1),
        TP_ESTADO_CIVIL INTEGER,
        TP_COR_RACA INTEGER,
        TP_NACIONALIDADE INTEGER,
        TP_ST_CONCLUSAO INTEGER,
        TP_ANO_CONCLUIU INTEGER,
        TP_ESCOLA INTEGER
    );
    """
    cursor.execute(create_table_query)

# Função para controlar o cancelamento
def solicitar_cancelamento():
    global cancelar
    cancelar = True
    print("Encerramento solicitado... Finalizando o processamento atual.")

 
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres", 
        password="postgres"   
    )
    return conn

# Função para inserir dados em lotes no PostgreSQL
def insert_batch(cursor, conn, data):
    try:
        query = """
        INSERT INTO enem_data (NU_INSCRICAO, NU_ANO, TP_FAIXA_ETARIA, TP_SEXO, TP_ESTADO_CIVIL, TP_COR_RACA, TP_NACIONALIDADE, TP_ST_CONCLUSAO, TP_ANO_CONCLUIU, TP_ESCOLA)
        VALUES %s
        ON CONFLICT (NU_INSCRICAO) DO NOTHING
        """
        extras.execute_values(cursor, query, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erro ao inserir no banco de dados: {e}")
        raise  

# Função para tentar a inserção repetidamente com controle de interrupção
def insert_with_retry(cursor, conn, data, progress_bar):
    global cancelar
    try:
        while not cancelar:
            try:
                insert_batch(cursor, conn, data)
                progress_bar.update(1)  
                break
            except psycopg2.OperationalError:
                print("Erro de conexão, tentando novamente...")
                time.sleep(5)
    except Exception as e:
        print(f"Erro na inserção com retry: {e}")

# Função que processa os dados e coloca na fila para inserção
def process_csv_chunk(caminho_arquivo, fila, total_chunks):
    global cancelar
    with tqdm(total=total_chunks, desc="Processando CSV") as pbar:  # Barra de progresso para processamento de CSV
        for i, chunk in enumerate(pd.read_csv(caminho_arquivo, sep=';', encoding='ISO-8859-1', chunksize=10000)):
            if cancelar:
                break  # Interrompe o processamento se o cancelamento foi solicitado
            print(f"Processando chunk {i+1}")
            # Tratamento dos dados
            data = [
                (
                    row['NU_INSCRICAO'], 
                    row['NU_ANO'], 
                    row['TP_FAIXA_ETARIA'], 
                    row['TP_SEXO'],
                    row['TP_ESTADO_CIVIL'],
                    row['TP_COR_RACA'],
                    row['TP_NACIONALIDADE'],
                    row['TP_ST_CONCLUSAO'],
                    row['TP_ANO_CONCLUIU'],
                    row['TP_ESCOLA']
                )
                for _, row in chunk.iterrows()
            ]
            fila.put(data)  # Coloca o lote na fila para inserção
            print(f"Chunk {i+1} processado e enviado para a fila")
            pbar.update(1)

# Função que consome da fila e insere no banco
def inserir_dados_na_fila(fila, progress_bar):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Verificar se a tabela existe e criá-la se não existir
    create_table_if_not_exists(cursor)
    conn.commit()  # Aplicar a criação da tabela, se for o caso

    while not cancelar:
        try:
            data = fila.get(timeout=5)
            if data is None:
                break
            print(f"Inserindo dados no banco... Tamanho do lote: {len(data)}")
            insert_with_retry(cursor, conn, data, progress_bar)
            fila.task_done()
            print(f"Lote inserido com sucesso.")
        except queue.Empty:
            if cancelar:
                break
        except Exception as e:
            print(f"Erro ao processar dados na thread: {e}")

    cursor.close()
    conn.close()

# Função para calcular o número de chunks no CSV
def calcular_total_chunks(caminho_arquivo, chunksize=10000):
    total_linhas = sum(1 for _ in open(caminho_arquivo, 'r', encoding='ISO-8859-1')) - 1  # Subtrai 1 para ignorar o cabeçalho
    total_chunks = (total_linhas // chunksize) + 1  # Calcula o número de chunks
    return total_chunks

# Main function
def main():
    global cancelar
    caminho_arquivo = 'MICRODADOS_ENEM_2023.csv'
    fila = queue.Queue(maxsize=20)

    total_chunks = calcular_total_chunks(caminho_arquivo)

    # Barra de progresso para a inserção no banco
    with tqdm(total=total_chunks, desc="Inserindo no Banco") as progress_bar:
        # Criação de threads para processar a fila
        threads = []
        for i in range(5):  # Criar 5 threads para inserção paralela
            t = threading.Thread(target=inserir_dados_na_fila, args=(fila, progress_bar,))
            t.daemon = True
            threads.append(t)
            t.start()

        try:
            # Processa o CSV e coloca os dados na fila
            process_csv_chunk(caminho_arquivo, fila, total_chunks)
            fila.join()  # Espera todas as threads concluírem
        except KeyboardInterrupt:
            solicitar_cancelamento()
            fila.join()

        # Enviar sinal de encerramento para as threads
        for _ in threads:
            fila.put(None)

        # Espera o término das threads
        for t in threads:
            t.join()

if __name__ == "__main__":
    main()
