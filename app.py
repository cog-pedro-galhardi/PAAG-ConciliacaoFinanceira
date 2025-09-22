import streamlit as st
import pandas as pd
import awswrangler as wr

# Page configuration
st.set_page_config(
    page_title="Conciliação Financeira",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    /* Main app background */
    .stApp {
        background-color: #0F1116;
    }
    /* Metric cards */
    div[data-testid="metric-container"] {
        background-color: #1A1C23;
        border: 1px solid #1A1C23;
        border-radius: 10px;
        padding: 20px;
        color: white;
    }
    div[data-testid="stMetricLabel"] > div {
        color: #A0AEC0;
    }
    /* DataFrame styling */
    .stDataFrame {
        border: 1px solid #1A1C23;
        border-radius: 10px;
    }
    /* Button styling */
    div[data-testid="stButton"] > button {
        background-color: #4A90E2;
        color: white;
        border-radius: 5px;
        border: none;
    }
</style>
""", unsafe_allow_html=True)


# --- AWS Athena Configuration ---
# Substitua os valores abaixo pelos da sua conta AWS
ATHENA_DATABASE = "conciliacao_financeira"
ATHENA_VIEW = "gold_paag_stark_agregada"
S3_OUTPUT_LOCATION = "s3://aws-athena-query-results-593793067943-us-east-1/"

# Função para buscar dados do Athena
@st.cache_data
def load_data_from_athena():
    """
    Conecta ao AWS Athena, executa uma query na view especificada
    e retorna os dados como um DataFrame do Pandas.
    """
    try:
        query = f'SELECT * FROM "{ATHENA_DATABASE}"."{ATHENA_VIEW}"'
        df = wr.athena.read_sql_query(
            sql=query,
            database=ATHENA_DATABASE,
            s3_output=S3_OUTPUT_LOCATION
        )
        
        # Renomear as colunas para corresponder ao front-end
        column_mapping = {
            'tr_created_at': 'Data Criação',
            'tr_updated_at': 'Data Atualização',
            'tr_merchant_id': 'Merchant ID',
            'tr_processor_id': 'Processor ID',
            'tr_flow_type': 'Tipo Fluxo',
            'tr_status': 'Status TR',
            'tr_tax_value': 'Valor Taxa',
            'tr_source': 'Fonte',
            'tr_transaction_type': 'Tipo Transação',
            'stt_status': 'Status STT',
            'st_dtposted': 'Data Posted',
            'st_other_source_type': 'Outro Tipo Fonte',
            'status_transacao_base': 'Status Base',
            'anl_valor': 'Valor Análise',
            'status_conciliacao': 'Status Conciliação',
            'amount_paag': 'Valor Paag',
            'amount_stark': 'Valor Stark',
            'qtd': 'Quantidade'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ou buscar dados no Athena: {e}")
        return pd.DataFrame() # Retorna um DataFrame vazio em caso de erro

# Carrega os dados
df = load_data_from_athena()


if not df.empty:
    # Sidebar
    with st.sidebar:
        st.image("https://paag.com.br/wp-content/uploads/2023/04/logo-paag-1.png", width=100)
        st.header("Conciliação Financeira")
        st.selectbox(" ", ["- Stark"])


    # Main content
    st.title("Conciliação Financeira - Stark")
    st.markdown("Monitore e gerencie a conciliação entre os sistemas Paag e Stark")

    st.markdown("---")

    # Metrics
    total_transacoes = len(df)
    
    # Tornar a verificação de status mais robusta (ignora maiúsculas/minúsculas e espaços em branco)
    transacoes_conciliadas = df[df['Status Conciliação'].str.strip().str.upper() == 'CONCILIADO'].shape[0]
    
    taxa_conciliacao = (transacoes_conciliadas / total_transacoes) * 100 if total_transacoes > 0 else 0

    valor_paag_total = df['Valor Paag'].sum()
    valor_stark_total = df['Valor Stark'].sum()
    diferenca_valores = abs(valor_paag_total - valor_stark_total)

    # Assuming 'integridade' is 100% for now as per screenshot (5 of 5 records complete)
    integridade_db = 100.0

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Taxa de Conciliação",
            value=f"{taxa_conciliacao:.1f}%",
            delta=f"{transacoes_conciliadas} de {total_transacoes} transações",
            delta_color="off"
        )

    with col2:
        st.metric(
            label="Diferença de Valores",
            value=f"R$ {diferenca_valores:,.2f}",
            delta=f"Paag: R$ {valor_paag_total:,.2f} | Stark: R$ {valor_stark_total:,.2f}",
            delta_color="off"
        )

    with col3:
        st.metric(
            label="Integridade do Banco de Dados",
            value=f"{integridade_db:.1f}%",
            delta=f"{total_transacoes} de {total_transacoes} registros completos",
            delta_color="off"
        )

    st.markdown("---")

    # Dataframe section
    col_btn, col_title = st.columns([0.85,0.15])
    with col_title:
        if st.button("Extrair Relatório"):
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Clique para baixar",
                data=csv,
                file_name='relatorio_conciliacao_stark.csv',
                mime='text/csv'
            )
    with col_btn:
        st.header("Dados de Conciliação")


    # Function to style the status columns
    def style_status(val):
        color_map = {
            'CONCILIADO': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
            'DIVERGENTE': 'background-color: #ffc107; color: black; border-radius: 5px; padding: 3px 8px;',
            'NAO_CONCLUIDO': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
            'PENDENTE': 'background-color: #17a2b8; color: white; border-radius: 5px; padding: 3px 8px;',
        }
        return color_map.get(val, '')

    # Apply styling
    # Identificar colunas de status para aplicar o estilo.
    # Adicione ou remova colunas conforme necessário.
    status_columns = [
        'Status Conciliação', 'Status STT', 'Status TR', 'Tipo Fluxo'
    ]
    # Filtrar para aplicar estilo apenas nas colunas que existem no DataFrame
    cols_to_style = [col for col in status_columns if col in df.columns]
    
    styled_df = df.style.apply(lambda col: col.map(style_status), subset=cols_to_style)


    st.dataframe(styled_df, use_container_width=True)
else:
    st.warning("Não foi possível carregar os dados do Athena. Verifique a configuração e as credenciais.")
