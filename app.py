import streamlit as st
import pandas as pd
import awswrangler as wr
import boto3
from datetime import datetime
import io

# Page configuration
st.set_page_config(
    page_title="Conciliação Financeira",
    page_icon="assets/cognitivo_ai_logo.jpg",
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
    Usa as credenciais guardadas no Streamlit Secrets.
    """
    try:
        # Cria uma sessão boto3 usando os segredos do Streamlit
        # Isso garante que a conexão seja autenticada na nuvem
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
            boto3_session=boto_session # Passa a sessão para o awswrangler
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
    # --- Conversão de Tipos de Dados ---
    # Garante que as colunas de data sejam do tipo datetime para a filtragem
    try:
        df['Data Criação'] = pd.to_datetime(df['Data Criação'])
        df['Data Atualização'] = pd.to_datetime(df['Data Atualização'])
    except Exception as e:
        st.error(f"Erro ao converter colunas de data: {e}")
        # Prossegue com os dados que puderam ser convertidos, mas avisa o usuário
    
    # Sidebar
    with st.sidebar:
        # Usa colunas para alinhar a imagem e o texto na mesma linha
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
    
    filt_col1, filt_col2 = st.columns([1, 2])

    with filt_col1:
        tipos_de_fluxo_disponiveis = df['Tipo Fluxo'].unique().tolist()
        
        # Tenta encontrar 'cashin' ou 'CREDITO' para definir como filtro padrão, ignorando o case.
        default_filter = []
        for fluxo in tipos_de_fluxo_disponiveis:
            if str(fluxo).strip().upper() in ['CASHIN', 'CREDITO']:
                default_filter.append(fluxo)
                break  # Pega o primeiro que encontrar e para

        fluxo_selecionado = st.multiselect(
            label="Tipo de Fluxo:",
            options=tipos_de_fluxo_disponiveis,
            default=default_filter
        )

    with filt_col2:
        # Define as datas mínima e máxima com base nos dados disponíveis
        min_date = df['Data Criação'].min().date()
        max_date = df['Data Criação'].max().date()

        # Cria uma sub-coluna para alinhar os campos de data
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


    # --- Lógica de Filtragem ---
    # 1. Filtra por tipo de fluxo
    if fluxo_selecionado:
        df_filtrado = df[df['Tipo Fluxo'].isin(fluxo_selecionado)]
    else:
        df_filtrado = df.copy()

    # 2. Filtra pelo período de data selecionado
    if not df_filtrado.empty and start_date and end_date:
        # Converte as datas do filtro para datetime para garantir a comparação correta
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        df_filtrado = df_filtrado[
            (df_filtrado['Data Criação'] >= start_datetime) & 
            (df_filtrado['Data Criação'] <= end_datetime)
        ]


    st.markdown("---")

    # Metrics - AGORA CALCULADAS COM BASE NOS DADOS FILTRADOS
    # Adicionamos uma verificação para o caso do filtro não retornar nenhum dado
    if not df_filtrado.empty:
        total_transacoes = len(df_filtrado)
        
        # Tornar a verificação de status mais robusta (ignora maiúsculas/minúsculas e espaços em branco)
        transacoes_conciliadas = df_filtrado[df_filtrado['Status Conciliação'].str.strip().str.upper() == 'CONCILIADO'].shape[0]
        
        taxa_conciliacao = (transacoes_conciliadas / total_transacoes) * 100 if total_transacoes > 0 else 0

        valor_paag_total = df_filtrado['Valor Paag'].sum()
        valor_stark_total = df_filtrado['Valor Stark'].sum()
        diferenca_valores = abs(valor_paag_total - valor_stark_total)

        # Lógica de placeholder para integridade, usando os dados filtrados
        integridade_db = 100.0
        integridade_registros = len(df_filtrado)

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
                delta=f"{integridade_registros} de {integridade_registros} registros completos",
                delta_color="off"
            )

        st.markdown("---")

        # --- Seção do DataFrame com Botões de Download ---
        title_col, btns_col = st.columns([0.6, 0.4], vertical_alignment="center")

        with title_col:
            st.markdown("### Dados de Conciliação")

        with btns_col:
            # Prepara os dados para download em memória
            csv_data = df_filtrado.to_csv(index=False).encode('utf-8')
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_filtrado.to_excel(writer, index=False, sheet_name='Conciliacao')
            excel_data = output.getvalue()
            
            # Cria colunas para os botões para que fiquem lado a lado
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

        # Função para estilizar as colunas de status com cores
        def style_status(val):
            """
            Aplica um estilo de fundo colorido a um valor de status.
            Normaliza o valor (remove espaços e converte para maiúsculas) para garantir a correspondência.
            """
            # Normaliza o valor para ser mais robusto
            normalized_val = str(val).strip().upper()
            
            color_map = {
                # Status de Conciliação e STT
                'CONCILIADO': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'DIVERGENTE': 'background-color: #ffc107; color: black; border-radius: 5px; padding: 3px 8px;',
                'NAO_CONCLUIDO': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'S - NST': 'background-color: #6f42c1; color: white; border-radius: 5px; padding: 3px 8px;', # Roxo
                'ST': 'background-color: #6f42c1; color: white; border-radius: 5px; padding: 3px 8px;', # Roxo
                'NS - NST': 'background-color: #6c757d; color: white; border-radius: 5px; padding: 3px 8px;', # Cinza
                
                # Status TR (APROVADO/REJEITADO/SUCCESS/FAILED/PENDING) - Verde para sucesso, Vermelho para falha, Amarelo para pendente
                'APROVADO': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'SUCCESS': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'SUCESSO': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'REJEITADO': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'FAILED': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'FAIL': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'FALHA': 'background-color: #dc3545; color: white; border-radius: 5px; padding: 3px 8px;',
                'PENDENTE': 'background-color: #ffc107; color: black; border-radius: 5px; padding: 3px 8px;',
                'PENDING': 'background-color: #ffc107; color: black; border-radius: 5px; padding: 3px 8px;',
                
                # Tipo de Fluxo (CRÉDITO/DÉBITO/CASHIN)
                'CREDITO': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'CASHIN': 'background-color: #28a745; color: white; border-radius: 5px; padding: 3px 8px;',
                'DEBITO': 'background-color: #17a2b8; color: white; border-radius: 5px; padding: 3px 8px;',
            }
            return color_map.get(normalized_val, '')

        status_columns = [
            'Status Conciliação', 'Status STT', 'Status TR', 'Tipo Fluxo'
        ]
        cols_to_style = [col for col in status_columns if col in df_filtrado.columns]
        
        styled_df = df_filtrado.style.apply(lambda col: col.map(style_status), subset=cols_to_style)


        st.dataframe(styled_df, use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")
else:
    st.warning("Não foi possível carregar os dados do Athena. Verifique a configuração e as credenciais.")


# --- Rodapé ---
st.markdown("---")
# Usa colunas para centralizar a logo e o texto no rodapé
col1, col2, col3 = st.columns([2, 3, 2])
with col2:
    st.markdown("<p style='text-align: center; color: grey;'>Desenvolvido pela Cognitivo.AI</p>", unsafe_allow_html=True)
