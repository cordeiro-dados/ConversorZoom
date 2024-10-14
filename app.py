import pandas as pd
import streamlit as st
import os

# Função para carregar os dados do Excel
def load_data(file_path):
    df = pd.read_excel(file_path)
    df["CodHistorico"] = "350"
    df['Data'] = pd.to_datetime(df['Data'], format='%d%m%Y').dt.strftime('%d%m%Y')
    return df

# Função para processar os dados
def process_data(df):
    data_rows = []

    for index, row in df.iterrows():
        data_rows.append([
            row['Data'], row['Debite'], row['Credite'], row['Valor'], str(row["CodHistorico"]),
            row['Histórico']
        ])
        xx_centro_custo = f"XX;{row['D/C']}"
        data_rows.append([
            xx_centro_custo, row['Centro de Custo'], row['Valor']
        ])

    return data_rows

# Função para salvar os dados processados em um CSV
def save_to_csv(data_rows, csv_file_path):
    with open(csv_file_path, 'w', newline='', encoding='utf-8-sig') as file:
        for row in data_rows:
            if row[0] == "XX:":
                file.write(",".join([str(item) for item in row if pd.notnull(item)]) + "\n")
            else:
                file.write(",".join([str(item) for item in row]) + "\n")

# Função principal usando Streamlit
def main():
    st.title("Conversor de Arquivo Excel para CSV")
    st.write("Selecione um arquivo Excel para processar e gerar um arquivo CSV.")

    # Carregar o arquivo usando Streamlit
    uploaded_file = st.file_uploader("Escolha o arquivo Excel", type=["xlsx", "xls"])

    if uploaded_file is not None:
        # Carrega os dados
        df = load_data(uploaded_file)

        # Exibe uma prévia do arquivo carregado
        st.write("Prévia do arquivo carregado:")
        st.dataframe(df.head())

        # Processa os dados
        data_rows = process_data(df)

        # Define o nome do arquivo CSV
        csv_file_name = os.path.splitext(uploaded_file.name)[0] + ".csv"

        # Botão para salvar o arquivo CSV processado
        if st.button("Salvar arquivo CSV"):
            save_to_csv(data_rows, csv_file_name)
            st.success(f"Arquivo CSV salvo com sucesso como: {csv_file_name}")
            st.download_button(label="Download CSV", data=open(csv_file_name, 'r', encoding='utf-8').read(), file_name=csv_file_name)

if __name__ == "__main__":
    main()
