import pandas as pd
import os
import glob



# uma função de extract que le e consolida no json

def extrair_dados_e_consolidar(path:str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path,'*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list,ignore_index=True)
    return df_total


# uma função que transforma
def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"] 
    return df



# uma função que da load em csv ou parquet
def carregar_dados(df: pd.DataFrame,formato_saida: list):
    """_summary_
    parametro que vai ser ou "csv" ou "parquet" ou "os dois"
    """
    for formato in formato_saida:
        if formato  == 'csv':
            df.to_csv("dados.csv")
        if formato  == 'parquet':
            df.to_parquet("dados.parquet")


def pipeline_calcular_kpi_de_vendas_consolidado(pasta:str,formato_de_saida:list):
    data_frame = extrair_dados_e_consolidar(pasta)
    data_frame2 = calcular_kpi_de_total_de_vendas(data_frame)
    carregar_dados(data_frame2,formato_de_saida)

