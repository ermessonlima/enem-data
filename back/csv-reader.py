import pandas as pd

def contar_linhas_e_colunas_csv(caminho_arquivo):
    try:
        # Lendo o arquivo CSV com pandas e contando as linhas e colunas
        df = pd.read_csv(caminho_arquivo, sep=';', encoding='ISO-8859-1')
        linhas, colunas = df.shape  # Obtém o número de linhas e colunas
        print(f"O arquivo CSV contém {linhas} linhas e {colunas} colunas.")
        return linhas, colunas
    except FileNotFoundError:
        print("Arquivo não encontrado!")
        return None
    except Exception as e:
        print(f"Erro ao ler o arquivo: {e}")
        return None

# Exemplo de uso
caminho_arquivo = "MICRODADOS_ENEM_2023.csv"
contar_linhas_e_colunas_csv(caminho_arquivo)
