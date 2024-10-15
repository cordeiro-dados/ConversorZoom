import streamlit as st
from databricks import sql
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder
import uuid

# Carrega variáveis de ambiente
load_dotenv()
DB_SERVER_HOSTNAME = os.getenv("DB_SERVER_HOSTNAME")
DB_HTTP_PATH = os.getenv("DB_HTTP_PATH")
DB_ACCESS_TOKEN = os.getenv("DB_ACCESS_TOKEN")

# Função para conectar ao banco de dados
def conectar_banco():
    try:
        conn = sql.connect(
            server_hostname=DB_SERVER_HOSTNAME,
            http_path=DB_HTTP_PATH,
            access_token=DB_ACCESS_TOKEN
        )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para inserir dados na tabela (Start)
def inserir_registro(conn, registro):
    try:
        cursor = conn.cursor()
        query = f"""
        INSERT INTO datalake.silver_pny.work_load (id, date, start_time, end_time, description, projeto)
        VALUES ('{registro['id']}', '{registro['date']}', '{registro['start_time']}', NULL, NULL, '{registro['projeto']}')
        """
        cursor.execute(query)
        cursor.close()
    except Exception as e:
        st.error(f"Erro ao inserir registro: {e}")

# Função para atualizar o registro com o End e Descrição
def atualizar_registro(conn, fim, descricao, projeto):
    try:
        cursor = conn.cursor()
        # Selecionar o último registro onde end_time é NULL e o projeto corresponde
        query_select = f"""
        SELECT id FROM datalake.silver_pny.work_load WHERE end_time IS NULL AND projeto = '{projeto}' ORDER BY start_time DESC LIMIT 1
        """
        cursor.execute(query_select)
        resultado = cursor.fetchone()

        if resultado:
            ultimo_registro_id = resultado[0]

            # Atualizar o registro com o end_time e a descrição
            query_update = f"""
            UPDATE datalake.silver_pny.work_load
            SET end_time = '{fim}', description = '{descricao}'
            WHERE id = '{ultimo_registro_id}'
            """
            cursor.execute(query_update)
            st.success(f"Ended at {fim}")
        else:
            st.error("Nenhum registro em aberto foi encontrado para finalizar.")
        
        cursor.close()
    except Exception as e:
        st.error(f"Erro ao atualizar registro: {e}")

# Função para carregar registros do banco de dados e calcular horas trabalhadas
def carregar_registros(conn, projeto):
    try:
        query = f"""
        SELECT id, date, start_time, end_time, description,
        TIMESTAMPDIFF(SECOND, start_time, end_time) AS total_seconds
        FROM datalake.silver_pny.work_load
        WHERE projeto = '{projeto}'
        """
        df = pd.read_sql(query, conn)
        
        # Calcular horas trabalhadas a partir dos segundos
        df['total_hours'] = df['total_seconds'].apply(lambda x: f"{x//3600}h {(x%3600)//60}min" if pd.notnull(x) else 'Em andamento')
        return df
    except Exception as e:
        st.error(f"Erro ao carregar registros: {e}")
        return pd.DataFrame()

# Função para carregar lista de projetos existentes
def carregar_projetos(conn):
    try:
        query = "SELECT DISTINCT projeto FROM datalake.silver_pny.work_load"
        df = pd.read_sql(query, conn)
        return df['projeto'].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar projetos: {e}")
        return []

# Função para exibir a tabela interativa
def exibir_tabela_interativa(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(paginationAutoPageSize=True)  # Paginação automática
    gb.configure_side_bar()  # Barra lateral com opções de filtro
    gb.configure_default_column(editable=False, filter=True)  # Habilitar filtros nas colunas
    grid_options = gb.build()

    # Exibe a tabela interativa com AgGrid
    AgGrid(df, gridOptions=grid_options, enable_enterprise_modules=True)

# Interface em Streamlit
st.title("ARKKADATA Work Log")

# Conectar ao banco de dados
conn = conectar_banco()

if conn:
    # Carregar projetos existentes
    projetos_existentes = carregar_projetos(conn)
    
    # Permitir seleção ou inserção de novo projeto
    projeto_selecionado = st.selectbox("Select a project or add a new one", ["New Project"] + projetos_existentes)
    
    if projeto_selecionado == "New Project":
        projeto_selecionado = st.text_input("Enter the name of the new project")

    if projeto_selecionado:
        # Inicializar os campos para manter o estado
        if 'start_time' not in st.session_state:
            st.session_state['start_time'] = ""
        if 'end_time' not in st.session_state:
            st.session_state['end_time'] = ""
        if 'descricao' not in st.session_state:
            st.session_state['descricao'] = ""

        # Inputs para o registro
        data = st.text_input("Date", datetime.now().strftime('%Y-%m-%d'), disabled=True)
        inicio = st.text_input("Start Time", st.session_state['start_time'], disabled=True)
        fim = st.text_input("End Time", st.session_state['end_time'], disabled=True)
        descricao = st.text_input("Description", st.session_state['descricao'])

        # Botão para Iniciar (Start)
        if st.button("Start"):
            agora = datetime.now().replace(microsecond=0)  # Remover microsegundos
            data = agora.strftime('%Y-%m-%d')
            st.session_state['start_time'] = agora.strftime('%Y-%m-%d %H:%M:%S')  # Atualiza o campo com a hora

            # Criar um novo registro
            novo_registro = {
                'id': str(uuid.uuid4()),
                'date': data,
                'start_time': st.session_state['start_time'],
                'projeto': projeto_selecionado
            }
            
            # Inserir registro no banco
            inserir_registro(conn, novo_registro)
            st.success(f"Started at {st.session_state['start_time']}")

        # Botão para Encerrar (End)
        if descricao and st.button("End"):
            agora = datetime.now().replace(microsecond=0)  # Remover microsegundos
            st.session_state['end_time'] = agora.strftime('%Y-%m-%d %H:%M:%S')  # Atualiza o campo com a hora

            # Atualizar o registro com o fim e a descrição
            atualizar_registro(conn, st.session_state['end_time'], descricao, projeto_selecionado)
        elif not descricao and st.button("End"):
            st.error("Por favor, insira a descrição antes de finalizar.")

        # Botão para Limpar Campos
        if st.session_state['end_time']:
            if st.button("Limpar Campos"):
                # Limpar os campos na session_state
                st.session_state['start_time'] = ""
                st.session_state['end_time'] = ""
                st.session_state['descricao'] = ""  # Adicionei a limpeza correta da descrição
                descricao = ""  # Limpa o campo de descrição
                st.success("Campos limpos!")

        # Exibir registros
        st.subheader(f"Records for project: {projeto_selecionado}")
        registros = carregar_registros(conn, projeto_selecionado)
        if not registros.empty:
            exibir_tabela_interativa(registros)

    # Fechar a conexão
    conn.close()
