import pandas as pd
import numpy as np
import re
from datetime import datetime
import pytz

# Definir o fuso horário brasileiro
fuso_brasil = pytz.timezone('America/Sao_Paulo')

# Obter a data e hora atual no fuso horário brasileiro
data_hora_atual = datetime.now(fuso_brasil).strftime('%Y-%m-%d %H:%M:%S')

print(f"{data_hora_atual} - Carregando a base\n")

# Carrega a base de dados
df = pd.read_csv(
    "VigiMed_Notificacoes.csv",
    encoding="ISO-8859-1", # para manter as acentuações
    delimiter=";", # separador
    dtype=str, # carrega tudo em string para tratar depois
    header=0 # primeira linha é o nome das colunas
)

# Mostra o DataFrame original
print(f"{data_hora_atual} - DF original carregado:")
print(df)
print("")

#------------------------------------------------------------------------------------------------------------------------------#
###             MOSTRA TODAS AS INFORMAÇÕES DAS COLUNAS         ###
#------------------------------------------------------------------------------------------------------------------------------#

# Inicializando as listas para os resultados
columns = []
data_types = []
total_counts = []
null_counts = []
none_counts = []
empty_string_counts = []
unique_counts = []

# Iterar sobre as colunas para calcular os valores
for column in df.columns:
    # Tipagem da coluna
    data_type = df[column].dtype

    # Total de valores na coluna
    total_count = len(df[column])

    # Contagem de valores nulos (NaN)
    null_count = df[column].isnull().sum()

    # Contagem de valores None
    none_count = (df[column] == None).sum()

    # Contagem de valores vazios (strings "")
    empty_string_count = (df[column] == "").sum()

    # Valores únicos
    unique_count = df[column].nunique(dropna=True)  # Exclui NaN na contagem

    # Adiciona os resultados às listas
    columns.append(column)
    data_types.append(str(data_type))
    total_counts.append(total_count)
    null_counts.append(null_count)
    none_counts.append(none_count)
    empty_string_counts.append(empty_string_count)
    unique_counts.append(unique_count)

# Criar o DataFrame com os resultados
summary_df = pd.DataFrame({
    "attribute": columns,
    "data_type": data_types,
    "total_count": total_counts,
    "null_count (NaN)": null_counts,
    "none_count": none_counts,
    "empty_string_count": empty_string_counts,
    "unique_count": unique_counts
})

# Mostrar o DataFrame de resumo
print("\nResumo das informações da tabela original:\n")
print(summary_df)
print("")

#------------------------------------------------------------------------------------------------------------------------------#
###             TESTE DELIMITADOR         ###
#------------------------------------------------------------------------------------------------------------------------------#

# Teste limitador
df = df[df["IDENTIFICACAO_NOTIFICACAO"] == "BR-ANVISA-300094478"]

#------------------------------------------------------------------------------------------------------------------------------#
###             CRIAÇÃO DO ATRIBUTO ORIGEM_NOTIFICACAO         ###
#------------------------------------------------------------------------------------------------------------------------------#

print(f"{data_hora_atual} - Criando o atributo ORIGEM_NOTIFICACAO\n")

# Ajustar a coluna "recebido_de" para tratar o caso do prefixo "Outro ..."
df["RECEBIDO_DE_AJUSTADO"] = df["RECEBIDO_DE"].apply(
    lambda x: "Outro" if isinstance(x, str) and x.startswith("Outro") else x
)

# Criar a coluna "origem_notificacao" conforme os critérios
def origem_notificacao(row):
    tipo_entrada = row["TIPO_ENTRADA_VIGIMED"]
    recebido_de = row["RECEBIDO_DE_AJUSTADO"]

    if pd.isnull(tipo_entrada) or tipo_entrada == "None":
        if pd.isnull(recebido_de) or recebido_de == "None":
            return None
        else:
            return recebido_de
    elif pd.notnull(tipo_entrada) and tipo_entrada != "None":
        if pd.notnull(recebido_de) and recebido_de != "None":
            return f"{tipo_entrada}-{recebido_de}"
        else:
            return tipo_entrada
    else:
        return recebido_de

df["ORIGEM_NOTIFICACAO"] = df.apply(origem_notificacao, axis=1)

# Remover a coluna intermediária "recebido_de_ajustado" se não for mais necessária
df.drop(columns=["RECEBIDO_DE_AJUSTADO"], inplace=True)

print(f"{data_hora_atual} - Criado o atributo ORIGEM_NOTIFICACAO\n")

#------------------------------------------------------------------------------------------------------------------------------#
###             CRIAÇÃO DO ATRIBUTO ANO_MES_NOTIFICACAO         ###
#------------------------------------------------------------------------------------------------------------------------------#

print(f"{data_hora_atual} - Criando o atributo ANO_MES_NOTIFICACAO\n")

# Criar a coluna ANO_MES_NOTIFICACAO
def calcular_ano_mes_notificacao(row):
    data_notificacao = row["DATA_NOTIFICACAO"]
    data_inclusao_sistema = row["DATA_INCLUSAO_SISTEMA"]

    if pd.isnull(data_notificacao) or data_notificacao == "None" or str(data_notificacao).startswith("0"):
        # Caso DATA_NOTIFICACAO inválida, usa DATA_INCLUSAO_SISTEMA
        return pd.to_datetime(str(data_inclusao_sistema)[:6], format="%Y%m", errors="coerce")
    elif len(str(data_notificacao)) == 8:
        # Caso DATA_NOTIFICACAO tenha 8 caracteres
        return pd.to_datetime(str(data_notificacao)[:6], format="%Y%m", errors="coerce")
    else:
        # Caso padrão: usa DATA_INCLUSAO_SISTEMA
        return pd.to_datetime(str(data_inclusao_sistema)[:6], format="%Y%m", errors="coerce")

# Aplicar a função para criar a nova coluna
df["ANO_MES_NOTIFICACAO"] = df.apply(calcular_ano_mes_notificacao, axis=1)

print(f"{data_hora_atual} - Criado o atributo ANO_MES_NOTIFICACAO\n")

#------------------------------------------------------------------------------------------------------------------------------#
###             AGRUPAMENTO DO ATRIBUTO TIPO_NOTIFICACAO         ###
#------------------------------------------------------------------------------------------------------------------------------#

print(f"{data_hora_atual} - Agrupando o atributo TIPO_NOTIFICACAO\n")

# Criar/Atualizar a coluna TIPO_NOTIFICACAO
df["TIPO_NOTIFICACAO"] = df["TIPO_NOTIFICACAO"].apply(
    lambda x: x if isinstance(x, str) and x.startswith("Not") else "Outro"
)

print(f"{data_hora_atual} - Agrupado o atributo TIPO_NOTIFICACAO\n")

#------------------------------------------------------------------------------------------------------------------------------#
###             CRIAÇÃO DO ATRIBUTO GRUPO_ETARIO         ###
#------------------------------------------------------------------------------------------------------------------------------#

print(f"{data_hora_atual} - Criando o atributo GRUPO_ETARIO\n")

# Regex para extrair número e unidade de tempo
number_pattern = r"(\d+)"
unit_pattern = r"(hora|dia|semana|mês|ano|década)"

# Extração de valor e período
df["valor"] = df["IDADE_MOMENTO_REACAO"].apply(
    lambda x: int(re.search(number_pattern, x).group(1)) if isinstance(x, str) and re.search(number_pattern, x) else np.nan
)
df["periodo"] = df["IDADE_MOMENTO_REACAO"].apply(
    lambda x: re.search(unit_pattern, x).group(1) if isinstance(x, str) and re.search(unit_pattern, x) else ""
)

# Extração do ano de notificação
df["ano_notificacao"] = df["ANO_MES_NOTIFICACAO"].fillna("0000").astype(str).str[:4].replace("0000", np.nan).astype(float)

# Cálculo de idade em dias
def calculate_age_in_days(row):
    if pd.isna(row["valor"]):
        if pd.notna(row["DATA_NASCIMENTO"]) and row["DATA_NASCIMENTO"] != "None":
            if str(row["DATA_NASCIMENTO"]).startswith("0"):
                return np.nan
            elif len(str(row["DATA_NASCIMENTO"])) == 4:
                return (row["ano_notificacao"] - int(row["DATA_NASCIMENTO"])) * 365
            elif len(str(row["DATA_NASCIMENTO"])) in [6, 8]:
                return (row["ano_notificacao"] - int(str(row["DATA_NASCIMENTO"])[:4])) * 365
            else:
                return np.nan
        else:
            return np.nan
    elif row["periodo"] == "":
        return row["valor"] * 365  # Assume que são anos
    elif row["periodo"] == "hora":
        return row["valor"] / 24  # Converte horas para dias
    elif row["periodo"] == "dia":
        return row["valor"]  # Mantém dias
    elif row["periodo"] == "semana":
        return row["valor"] * 7  # Converte semanas para dias
    elif row["periodo"] == "mês":
        return row["valor"] * 30  # Converte meses para dias
    elif row["periodo"] == "ano":
        return row["valor"] * 365  # Converte anos para dias
    elif row["periodo"] == "década":
        return row["valor"] * 3650  # Converte décadas para dias
    else:
        return np.nan

df["idade_em_dias"] = df.apply(calculate_age_in_days, axis=1)

# Determinação do grupo etário
def determina_grupo_idade(idade_em_dias):
    if pd.isna(idade_em_dias):
        return np.nan
    elif idade_em_dias <= 20:
        return "Neonato"
    elif idade_em_dias <= 180:
        return "Lactente"
    elif idade_em_dias <= 7 * 365:
        return "Criança"
    elif idade_em_dias <= 16 * 365:
        return "Adolescente"
    elif idade_em_dias <= 64 * 365:
        return "Adulto"
    else:
        return "Idoso"

df["GRUPO_ETARIO"] = df["idade_em_dias"].apply(determina_grupo_idade)

# Ajuste do grupo etário com base em "GRUPO_IDADE"
def ajusta_grupo_etario(row):
    if pd.isna(row["GRUPO_ETARIO"]):
        if pd.isna(row["GRUPO_IDADE"]) or row["GRUPO_IDADE"] == "None":
            return "NaN"
        elif row["GRUPO_IDADE"] == "Feto":
            return "Neonato"
        elif row["GRUPO_IDADE"] == "Infantil":
            return "Lactente"
        else:
            return row["GRUPO_IDADE"]
    return row["GRUPO_ETARIO"]

df["GRUPO_ETARIO"] = df.apply(ajusta_grupo_etario, axis=1)

# Remover colunas temporárias
df.drop(columns=["valor", "periodo", "ano_notificacao", "idade_em_dias"], inplace=True)

print(f"{data_hora_atual} - Criado o atributo GRUPO_ETARIO\n")

#------------------------------------------------------------------------------------------------------------------------------#
###             AGRUPAMENTO DO ATRIBUTO SEXO         ###
#------------------------------------------------------------------------------------------------------------------------------#

print(f"{data_hora_atual} - Agrupando o atributo SEXO\n")

# Substituindo valores nulos ou "None" por "Desconhecido" na coluna "sexo"
df['SEXO'] = df['SEXO'].fillna('Desconhecido').replace('None', 'Desconhecido')

print(f"{data_hora_atual} - Agrupado o atributo SEXO\n")

#------------------------------------------------------------------------------------------------------------------------------#
###             EXPLOSÃO DOS ATRIBUTOS CONCATENADOS         ###
#------------------------------------------------------------------------------------------------------------------------------#

print(f"{data_hora_atual} - Iniciando a explosão dos atributos:\n")

# Configurações iniciais
colunas_para_explodir = [
    "REACAO_EVENTO_ADVERSO_MEDDRA",
    "GRAVE",
    "GRAVIDADE",
    "DESFECHO",
    "RELACAO_MEDICAMENTO_EVENTO",
    "NOME_MEDICAMENTO_WHODRUG",
    "ACAO_ADOTADA"
]

colunas_para_copiar = [
    "REACAO_EVENTO_ADVERSO_MEDDRA",
    "GRAVE",
    "GRAVIDADE",
    "DESFECHO"
]

delimitador = "|"  # Delimitador principal
delimitador_gravidade = ","  # Delimitador adicional para GRAVIDADE

# Criar um DataFrame vazio para armazenar os dados processados
df_expandido = pd.DataFrame()

# Processar linha por linha
for index, row in df.iterrows():
    # Delimitador para teste
    # if index == 1000:
    #     break
    print(f"Processando linha: {index}")

    # Explodir colunas selecionadas pelo delimitador principal
    valores_explodidos = {
        coluna: str(row[coluna]).replace("_x000D_", "").split(delimitador) if pd.notna(row[coluna]) else [None]
        for coluna in colunas_para_explodir
    }

    # Determinar o número máximo de valores entre as colunas explodidas
    max_linhas = max(len(valores) for valores in valores_explodidos.values())

    # Gerar novas linhas a partir das colunas explodidas
    novas_linhas = []
    for i in range(max_linhas):
        nova_linha = row.copy()
        for coluna in colunas_para_explodir:
            nova_linha[coluna] = valores_explodidos[coluna][i] if i < len(valores_explodidos[coluna]) else None
        novas_linhas.append(nova_linha)

    # Adicionar as novas linhas ao DataFrame intermediário
    df_temp = pd.DataFrame(novas_linhas)

    # Explodir novamente a coluna GRAVIDADE pelo delimitador adicional
    if "GRAVIDADE" in df_temp.columns:
        gravidade_expandido = []
        for _, temp_row in df_temp.iterrows():
            gravidade_valores = (
                str(temp_row["GRAVIDADE"]).split(delimitador_gravidade) if pd.notna(temp_row["GRAVIDADE"]) else [None]
            )
            for gravidade_valor in gravidade_valores:
                nova_linha = temp_row.copy()
                nova_linha["GRAVIDADE"] = gravidade_valor
                gravidade_expandido.append(nova_linha)
        df_temp = pd.DataFrame(gravidade_expandido)

    # Adicionar ao DataFrame final
    df_expandido = pd.concat([df_expandido, df_temp], ignore_index=True)

# Tratar linhas incompletas para cada `IDENTIFICACAO_NOTIFICACAO`
for id_notificacao in df_expandido["IDENTIFICACAO_NOTIFICACAO"].unique():
    df_id = df_expandido[df_expandido["IDENTIFICACAO_NOTIFICACAO"] == id_notificacao]
    ultima_linha_valida = None

    for index, row in df_id.iterrows():
        # Verificar se as colunas explodidas são nulas ou None
        if all(pd.isna(row[coluna]) or row[coluna] is None for coluna in colunas_para_copiar):
            # Substituir os valores das colunas explodidas pela última linha válida
            if ultima_linha_valida is not None:
                for coluna in colunas_para_copiar:
                    df_expandido.loc[row.name, coluna] = ultima_linha_valida[coluna]
        else:
            # Atualizar a última linha válida
            ultima_linha_valida = row

# Se GRAVIDADE estiver nulo e em "GRAVE" estiver como 'Não', então preenche com 'Sem gravidade'
df_expandido["GRAVIDADE"] = df_expandido.apply(
    lambda row: "Sem gravidade" if (pd.isna(row["GRAVIDADE"]) or row["GRAVIDADE"] in ["", "None"]) and row["GRAVE"] == "Não"
    else row["GRAVIDADE"],
    axis=1
)

print(f"{data_hora_atual} - Explosão concluída\n")

#------------------------------------------------------------------------------------------------------------------------------#
###             AGRUPA VALORES "None", "" E None PARA NaN         ###
#------------------------------------------------------------------------------------------------------------------------------#

print(f"{data_hora_atual} - Agrupando valores nulos e vazios\n")

# Substitui "None", strings vazias e None por NaN em todas as colunas
df_expandido = df_expandido.replace(["", "None", None], np.nan)

print(f"{data_hora_atual} - Agrupamento concluído\n")

#------------------------------------------------------------------------------------------------------------------------------#
###             CRIA O DATAFRAME FINAL SOMENTE COM AS COLUNAS QUE SERÃO UTILIZADAS         ###
#------------------------------------------------------------------------------------------------------------------------------#

# Atributos selecionados
colunas_selecionadas = [
    "IDENTIFICACAO_NOTIFICACAO",
    "ORIGEM_NOTIFICACAO",
    "ANO_MES_NOTIFICACAO",
    "TIPO_NOTIFICACAO",
    "GRUPO_ETARIO",
    "SEXO",
    "REACAO_EVENTO_ADVERSO_MEDDRA",
    "GRAVE",
    "GRAVIDADE",
    "DESFECHO",
    "RELACAO_MEDICAMENTO_EVENTO",
    "NOME_MEDICAMENTO_WHODRUG",
    "ACAO_ADOTADA"
]

df_final = df_expandido[colunas_selecionadas]

#------------------------------------------------------------------------------------------------------------------------------#
###             REMOVE ESPAÇOS EM BRANCO ANTES E DEPOIS DAS STRINGS         ###
#------------------------------------------------------------------------------------------------------------------------------#

# Remove espaços em branco antes e depois dos valores no DataFrame expandido
df_final = df_final.applymap(lambda x: x.strip() if isinstance(x, str) else x)

#------------------------------------------------------------------------------------------------------------------------------#
###             MOSTRA A QUANTIDADE DE INSTÂNCIAS EM CADA VALOR DAS COLUNAS         ###
#------------------------------------------------------------------------------------------------------------------------------#

# Exibe a quantidade de cada valor de cada coluna do DataFrame final
for column in df_final.columns:
    print(f"\nQuantidade de valores na coluna '{column}':\n")
    print(df_final[column].value_counts(dropna=False))
    print("")

#------------------------------------------------------------------------------------------------------------------------------#
###             SALVA O DATAFRAME FINAL         ###
#------------------------------------------------------------------------------------------------------------------------------#

df_final.to_csv(
    f"notificacoes_clean.csv",
    encoding="ISO-8859-1", # para manter as acentuações
    sep=";", # separador
    index=False # Não incluir o índice
)

print(f"{data_hora_atual} - Algoritmo concluído")


