import streamlit as st
import pandas as pd
import awswrangler as wr
import boto3
from datetime import datetime
import io

st.set_page_config(
    page_title="Conciliação Financeira",
    page_icon="assets/cognitivo_ai_logo.jpg",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
    /* Button styling (geral e download) */
    div[data-testid="stButton"] > button, div[data-testid="stDownloadButton"] > button {
        background-color: #4A90E2;
        color: white !important; /* !important para sobrescrever o tema light */
        border-radius: 5px;
        border: 1px solid #4A90E2;
    }
    /* Oculta as linhas horizontais da grade do DataFrame tornando a borda transparente */
    :root {
        --gdg-border-color: transparent;
    }
    /* Garante que os filtros mantenham o fundo escuro e texto claro */
    div[data-testid="stMultiSelect"] > div, div[data-testid="stDateInput"] > div > div {
        background-color: #1A1C23;
    }
    div[data-testid="stMultiSelect"] label, div[data-testid="stDateInput"] label {
        color: #A0AEC0 !important;
    }
    div[data-testid="stMultiSelect"] input, div[data-testid="stDateInput"] input {
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)

ATHENA_DATABASE = "conciliacao_financeira"
ATHENA_VIEW = "gold_paag_stark_agregada"
S3_OUTPUT_LOCATION = "s3://aws-athena-query-results-593793067943-us-east-1/"

@st.cache_data
def load_data_from_athena():
    try:
        boto_session = boto3.Session(
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
            region_name=st.secrets["AWS_REGION"]
        )
        
        query = f'SELECT * FROM "{ATHENA_DATABASE}"."{ATHENA_VIEW}"'
        df = wr.athena.read_sql_query(
            sql=query,
            database=ATHENA_DATABASE,
            s3_output=S3_OUTPUT_LOCATION,
            boto3_session=boto_session
        )
        
        # Renomear as colunas para corresponder ao front
        column_mapping = {
            'tr_created_at': 'Data Criação',
            'tr_updated_at': 'Data Atualização',
            'tr_merchant_id': 'Merchant ID',
            'ms_name': 'MS Nome',
            'ms_licence': 'MS Licença',
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
        return pd.DataFrame()

@st.cache_data
def get_integrity_counts():
    try:
        boto_session = boto3.Session(
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
            region_name=st.secrets["AWS_REGION"]
        )
        
        tables_to_count = {
            "validos": "stg_transactions",
            "duplicados": "stg_transactions_duplicadas",
            "nulos": "stg_transactions_nulos"
        }
        
        counts = {}
        for key, table in tables_to_count.items():
            query = f"""
                SELECT COUNT(*) as count 
                FROM "{ATHENA_DATABASE}"."{table}" ts
                LEFT JOIN "{ATHENA_DATABASE}"."stg_processors" p ON ts.processor_id = p.id
                WHERE UPPER(p.processor_type) = 'STARK'
            """
            df_count = wr.athena.read_sql_query(
                sql=query,
                database=ATHENA_DATABASE,
                s3_output=S3_OUTPUT_LOCATION,
                boto3_session=boto_session
            )
            counts[key] = df_count['count'][0]
        
        total_registros = sum(counts.values())
        registros_validos = counts.get("validos", 0)
            
        return registros_validos, total_registros
    except Exception as e:
        st.error(f"Erro ao calcular a integridade do banco de dados: {e}")
        return 0, 0

# Carrega os dados
df = load_data_from_athena()
registros_validos, total_registros_base = get_integrity_counts()


if not df.empty:

    if total_registros_base > 0:
        integridade_db = (registros_validos / total_registros_base) * 100
    else:
        integridade_db = 0.0

    try:
        df['Data Criação'] = pd.to_datetime(df['Data Criação'])
        df['Data Atualização'] = pd.to_datetime(df['Data Atualização'])
    except Exception as e:
        st.error(f"Erro ao converter colunas de data: {e}")
    
    # --- Sidebar ---
    with st.sidebar:
        logo_col, title_col = st.columns([1, 3], vertical_alignment="center")
        with logo_col:
            st.image("assets/paag-logo.png", width=80)
        with title_col:
            st.markdown("### Conciliação Financeira")

        st.selectbox(" ", ["Stark"])

    # --- Título Principal ---
    st.title("Conciliação Financeira - Stark")
    st.markdown("Monitore e gerencie a conciliação entre os sistemas Paag e Stark")

    # --- Filtros ---
    st.markdown("### Filtros")
    
    filt_col1, filt_col2, filt_col3, filt_col4 = st.columns([1, 1, 1, 2])

    with filt_col1:
        tipos_de_fluxo_disponiveis = df['Tipo Fluxo'].unique().tolist()
        
        # Tenta encontrar 'cashin' para definir como filtro padrão, ignorando o case.
        default_filter = []
        for fluxo in tipos_de_fluxo_disponiveis:
            if str(fluxo).strip().upper() in ['CASHIN']:
                default_filter.append(fluxo)
                break

        fluxo_selecionado = st.multiselect(
            label="Tipo de Fluxo:",
            options=tipos_de_fluxo_disponiveis,
            default=default_filter
        )

    with filt_col2:
        status_tr_disponiveis = sorted(df['Status TR'].astype(str).unique().tolist())
        status_tr_selecionado = st.multiselect(
            label="Status TR:",
            options=status_tr_disponiveis,
            default=[]
        )

    with filt_col3:
        status_conciliacao_disponiveis = sorted(df['Status Conciliação'].astype(str).unique().tolist())
        status_conciliacao_selecionado = st.multiselect(
            label="Status Conciliação:",
            options=status_conciliacao_disponiveis,
            default=[]
        )

    with filt_col4:
        min_date = df['Data Criação'].min().date()
        max_date = df['Data Criação'].max().date()

        date_col1, date_col2 = st.columns(2)
        with date_col1:
            start_date = st.date_input(
                "Data de Início:",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
                key='start_date'
            )
        with date_col2:
            end_date = st.date_input(
                "Data de Fim:",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                key='end_date'
            )


    df_filtrado = df.copy()

    # 1. Filtra por tipo de fluxo
    if fluxo_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Tipo Fluxo'].isin(fluxo_selecionado)]

    # 2. Filtra por Status TR
    if status_tr_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Status TR'].isin(status_tr_selecionado)]
    
    # 3. Filtra por Status Conciliação
    if status_conciliacao_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Status Conciliação'].isin(status_conciliacao_selecionado)]

    # 4. Filtra pelo período de data selecionado
    if not df_filtrado.empty and start_date and end_date:
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        df_filtrado = df_filtrado[
            (df_filtrado['Data Criação'] >= start_datetime) & 
            (df_filtrado['Data Criação'] <= end_datetime)
        ]


    st.markdown("---")


    if not df_filtrado.empty:
        total_transacoes = df_filtrado['Quantidade'].sum()
        transacoes_conciliadas = df_filtrado[df_filtrado['Status Conciliação'].str.strip().str.upper() == 'CONCILIADO']['Quantidade'].sum()
        taxa_conciliacao = (transacoes_conciliadas / total_transacoes) * 100 if total_transacoes > 0 else 0
        valor_paag_total = df_filtrado['Valor Paag'].sum()
        valor_stark_total = df_filtrado['Valor Stark'].sum()
        diferenca_valores = abs(valor_paag_total - valor_stark_total)

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
                delta=f"{registros_validos:n} de {total_registros_base:n} registros",
                delta_color="off"
            )

        st.markdown("---")
        title_col, btns_col = st.columns([0.6, 0.4], vertical_alignment="center")

        with title_col:
            st.markdown("### Dados de Conciliação")

        with btns_col:
            csv_data = df_filtrado.to_csv(index=False).encode('utf-8')
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_filtrado.to_excel(writer, index=False, sheet_name='Conciliacao')
            excel_data = output.getvalue()
            
            dl_col1, dl_col2 = st.columns(2)
            with dl_col1:
                st.download_button(
                    label="⬇️ Baixar CSV",
                    data=csv_data,
                    file_name='relatorio_conciliacao.csv',
                    mime='text/csv',
                    use_container_width=True
                )
            with dl_col2:
                st.download_button(
                    label="⬇️ Baixar XLSX",
                    data=excel_data,
                    file_name='relatorio_conciliacao.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    use_container_width=True
                )

        def style_status(val):
            normalized_val = str(val).strip().upper()
            
            color_map = {
                # Status de Conciliação e STT
                'CONCILIADO': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',# Verde
                'DIVERGENTE': 'background-color: #ffc107; color: black; border-radius: 5px; padding: 3px 8px;',# Amarelo 
                'NAO_CONCLUIDO': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'S - NST': 'background-color: #6f42c1; color: white; border-radius: 5px; padding: 3px 8px;', # Roxo
                'ST': 'background-color: #6f42c1; color: white; border-radius: 5px; padding: 3px 8px;', # Roxo
                'NS - NST': 'background-color: #6c757d; color: white; border-radius: 5px; padding: 3px 8px;', # Cinza
                
                # Status TR (APROVADO/REJEITADO/SUCCESS/FAILED/PENDING)
                'APROVADO': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'SUCCESS': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'SUCESSO': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'REJEITADO': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'FAILED': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'FAIL': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'FALHA': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'PENDENTE': 'background-color: #ffc107; color: black; border-radius: 5px; padding: 3px 8px;',
                'PENDING': 'background-color: #ffc107; color: black; border-radius: 5px; padding: 3px 8px;',
                
                # Tipo de Fluxo (CASHOUT/CASHIN)
                'CASHIN': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'CASHOUT': 'background-color: #17a2b8; color: white; border-radius: 5px; padding: 3px 8px;',
            }
            return color_map.get(normalized_val, '')

        status_columns = [
            'Status Conciliação', 'Status STT', 'Status TR', 'Tipo Fluxo'
        ]
        cols_to_style = [col for col in status_columns if col in df_filtrado.columns]
        
        styled_df = df_filtrado.style.apply(lambda col: col.map(style_status), subset=cols_to_style)


        st.dataframe(styled_df, use_container_width=True, hide_index=True)
    else:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")
else:
    st.warning("Não foi possível carregar os dados do Athena. Verifique a configuração e as credenciais.")


# --- Rodapé ---
st.markdown("---")
col1, col2, col3 = st.columns([2, 3, 2])
with col2:
    st.markdown("<p style='text-align: center; color: grey;'>Desenvolvido pela Cognitivo.AI</p>", unsafe_allow_html=True)
