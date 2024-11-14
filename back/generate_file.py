## Código completo

## Importar as bibliotecas que serão usadas
import pandas as pd

def limpeza(chunk):

    ## Seleção de colunas que estão na modelagem 
    chunk = chunk[["NU_INSCRICAO",
                        "TP_ESCOLA",
                        "TP_COR_RACA",
                        "TP_SEXO",
                        "TP_FAIXA_ETARIA",
                        "IN_TREINEIRO",
                        "CO_MUNICIPIO_ESC",
                        "SG_UF_ESC",
                        "TP_PRESENCA_CN",
                        "TP_PRESENCA_CH",
                        "TP_PRESENCA_LC",
                        "TP_PRESENCA_MT",
                        "NU_NOTA_REDACAO",
                        "NU_NOTA_CN",
                        "NU_NOTA_CH",
                        "NU_NOTA_LC",
                        "NU_NOTA_MT",
    ]]

    ## Remoção de nulos:
    # Preenchendo com 0 todos os campos que possuem valores nulos. Na dimensão de Estados foi adicionada a categoria de "Não Informado", com código 0
    # Colunas com dados nulos: NU_NOTA_REDACAO	NU_NOTA_CN	NU_NOTA_CH	NU_NOTA_LC	NU_NOTA_MT, CO_MUNICIPIO_ESC, CO_UF_ESC
    # df_microdados_enem[['NU_NOTA_REDACAO', 'NU_NOTA_CN', 'NU_NOTA_MT', 'NU_NOTA_CH', 'NU_NOTA_LC', 'CO_MUNICIPIO_ESC', 'CO_UF_ESC'][] = df_microdados_enem[['NU_NOTA_REDACAO', 'NU_NOTA_CN', 'NU_NOTA_MT', 'NU_NOTA_CH', 'NU_NOTA_LC', 'CO_MUNICIPIO_ESC', 'CO_UF_ESC']].fillna(0)
    chunk = chunk.fillna(0)

    ## Criação de uma coluna de situação geral de ausencia ou presença por candidato e Remoção dascolunas que não serão mais usadas
    # Mesmo que um candidato tenha ido a 1 dia de prova, será considerado como ausente
    chunk.loc[:,'TP_PRESENCA'] = chunk.loc[:,'TP_PRESENCA_CN'] * chunk.loc[:,'TP_PRESENCA_CH'] * chunk.loc[:,'TP_PRESENCA_MT'] * chunk.loc[:,'TP_PRESENCA_LC']
    chunk.drop(columns={'TP_PRESENCA_CH', 'TP_PRESENCA_CN', 'TP_PRESENCA_MT', 'TP_PRESENCA_LC'}, inplace=True)

    # Criação de coluna geral de média
    chunk.loc[:,'MEDIA_GERAL'] = (chunk.loc[:,'NU_NOTA_CN'] + chunk.loc[:,'NU_NOTA_CH'] + chunk.loc[:,'NU_NOTA_MT'] +  chunk.loc[:,'NU_NOTA_LC'] + chunk.loc[:,'NU_NOTA_LC'])/5

    return chunk

## Para execução do 
contador = 0 
repeticoes = 200 # 

tamanho_chunk = 100000

for chunk in pd.read_csv('MICRODADOS_ENEM_2023.csv', sep=';', encoding_errors='ignore', chunksize=tamanho_chunk):
    df_microdados_enem_limpos = limpeza(chunk)

    try: # Concatenar Novos dados limpos com dados previamente transformados
        df_microdados_enem_limpos_completo = pd.concat([df_microdados_enem_limpos_completo, df_microdados_enem_limpos])

    except: # Caso não exista ainda o dataframe que será o completo, o except serve para efetivamente criar este novo dataframe a ser completo
        print('entrou no except')
        df_microdados_enem_limpos_completo = df_microdados_enem_limpos.iloc[:] # Usando o método iloc para perfomance

    if repeticoes < repeticoes:
        break

    repeticoes += 1


df_microdados_enem_limpos_completo.to_csv('microdados_enem_2020_l.csv', sep=';', index=False)