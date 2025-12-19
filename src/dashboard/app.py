import streamlit as st
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.dashboard.data_loader import ReportLoader
from src.dashboard.charts import (
    create_quality_score_timeline,
    create_metrics_breakdown,
    create_anomalies_by_method,
    create_severity_pie,
    create_metrics_heatmap,
    create_anomaly_trend
)

# Configuração da página
st.set_page_config(
    page_title="Monitor de Qualidade de Dados",
    page_icon="✶",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #2E86AB;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 5px solid #2E86AB;
    }
    .success-box {
        background-color: #d4edda;
        border-left: 5px solid #28a745;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border-left: 5px solid #ffc107;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border-left: 5px solid #dc3545;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_loader():
    return ReportLoader()

loader = get_loader()

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/clouds/200/000000/bar-chart.png", width=150)
    st.title(" Monitor de Dados")
    st.markdown("---")
    
    # Opções de navegação
    page = st.radio(
        "Navegação",
        [" Home", " Métricas de Qualidade", " Anomalias", " Histórico"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    

    if st.button("Atualizar Dados", use_container_width=True):
        st.cache_resource.clear()
        st.rerun()
    
    # Info
    st.markdown("---")
    st.caption("Sistema de Monitoramento")
    st.caption("Versão 1.0.0")

# Header
st.markdown('<h1 class="main-header"> Monitor de Qualidade de Dados</h1>', unsafe_allow_html=True)

# Carrega dados
try:
    summary = loader.get_summary_stats()
    metrics_df = loader.get_metrics_dataframe()
    anomalies_df = loader.get_anomalies_dataframe()
    
    has_data = not metrics_df.empty and not anomalies_df.empty
    
except Exception as e:
    st.error(f"Erro ao carregar dados: {e}")
    has_data = False

# Home
if page == " Home":
    
    if not has_data:
        st.warning(" Nenhum dado disponível ainda.")
        st.stop()
    
    # Indicadores principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Quality Score",
            value=f"{summary['quality_score']:.2f}%",
            delta=f"{summary['total_executions']} execuções"
        )
    
    with col2:
        st.metric(
            label="Anomalias Detectadas",
            value=summary['total_anomalies'],
            delta=f"{summary['anomaly_percentage']:.2f}%"
        )
    
    with col3:
        status_icon = "✅" if summary['alert_sent'] else "❌"
        st.metric(
            label="Status do Alerta",
            value=status_icon,
            delta="Slack"
        )
    
    with col4:
        st.metric(
            label="Última Execução",
            value=summary['last_execution'][:10] if summary['last_execution'] else "N/A",
            delta=summary['status']
        )
    
    st.markdown("---")
    
    # Gráficos principais
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(
            create_quality_score_timeline(metrics_df),
            use_container_width=True
        )
    
    with col2:
        st.plotly_chart(
            create_metrics_breakdown(metrics_df),
            use_container_width=True
        )
    
    # Status box
    if summary['quality_score'] >= 95:
        st.markdown(
            '<div class="success-box"><strong>Excelente!</strong> Qualidade dos dados acima de 95%.</div>',
            unsafe_allow_html=True
        )
    elif summary['quality_score'] >= 85:
        st.markdown(
            '<div class="warning-box"> <strong>Atenção:</strong> Qualidade dos dados entre 85-95%.</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div class="error-box"> <strong>Crítico:</strong> Qualidade dos dados abaixo de 85%!</div>',
            unsafe_allow_html=True
        )

# Métricas
elif page == " Métricas de Qualidade":
    
    if not has_data:
        st.warning(" Nenhum dado disponível ainda.")
        st.stop()
    
    st.header(" Análise de Métricas de Qualidade")
    
    # Timeline
    st.subheader("Evolução Temporal")
    st.plotly_chart(
        create_quality_score_timeline(metrics_df),
        use_container_width=True
    )
    
    # Heatmap e Breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(
            create_metrics_heatmap(metrics_df),
            use_container_width=True
        )
    
    with col2:
        st.plotly_chart(
            create_metrics_breakdown(metrics_df),
            use_container_width=True
        )
    
    # Tabela de histórico
    st.subheader("Histórico Detalhado")
    
    display_df = metrics_df[['timestamp', 'quality_score', 'completeness', 
                              'uniqueness', 'validity', 'consistency', 'total_rows']]
    display_df.columns = ['Data', 'Quality Score', 'Completude', 
                           'Unicidade', 'Validade', 'Consistência', 'Total Linhas']
    
    st.dataframe(
        display_df.style.background_gradient(cmap='RdYlGn', subset=['Quality Score']),
        use_container_width=True,
        hide_index=True
    )

# Anomalias
elif page == " Anomalias":
    
    if not has_data:
        st.warning("Nenhum dado disponível ainda.")
        st.stop()
    
    st.header(" Detecção de Anomalias")
    
    # Tendência
    st.subheader("Tendência de Anomalias")
    st.plotly_chart(
        create_anomaly_trend(anomalies_df),
        use_container_width=True
    )
    
    # Por método e severidade
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(
            create_anomalies_by_method(anomalies_df),
            use_container_width=True
        )
    
    with col2:
        st.plotly_chart(
            create_severity_pie(anomalies_df),
            use_container_width=True
        )
    
    # Tabela de anomalias
    st.subheader("Histórico de Anomalias")
    
    display_df = anomalies_df[['timestamp', 'total_anomalies', 'anomaly_percentage',
                                'zscore', 'iqr', 'isolation_forest',
                                'severity_high', 'severity_medium', 'severity_low']]
    display_df.columns = ['Data', 'Total', '% Anomalias', 
                           'Z-Score', 'IQR', 'Isolation Forest',
                           'Alta', 'Média', 'Baixa']
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# Histórico
elif page == "📋 Histórico":
    
    if not has_data:
        st.warning("Nenhum dado disponível ainda.")
        st.stop()
    
    st.header("Histórico de Execuções")
    
    # Visão geral
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total de Execuções", summary['total_executions'])
    
    with col2:
        time_range = loader.get_time_range()
        if time_range[0]:
            days = (time_range[1] - time_range[0]).days
            st.metric("Período", f"{days} dias")
    
    with col3:
        avg_score = metrics_df['quality_score'].mean()
        st.metric("Score Médio", f"{avg_score:.2f}%")
    
    st.markdown("---")
    
    # Abas de visualização
    tab1, tab2, tab3 = st.tabs([" Métricas", " Anomalias", " Relatórios"])
    
    with tab1:
        st.dataframe(
            metrics_df.style.background_gradient(cmap='RdYlGn', subset=['quality_score']),
            use_container_width=True,
            hide_index=True
        )
    
    with tab2:
        st.dataframe(anomalies_df, use_container_width=True, hide_index=True)
    
    with tab3:
        # Lista todos os relatórios
        reports = loader.load_final_reports()
        
        if reports:
            st.subheader(f" {len(reports)} Relatórios Disponíveis")
            
            for idx, report in enumerate(reversed(reports), 1):
                with st.expander(f"Relatório #{len(reports) - idx + 1} - {report['execution_info']['timestamp'][:19]}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.json(report['quality_metrics'])
                    
                    with col2:
                        st.json(report['anomaly_report'])

# rodapé
st.markdown("---")
st.caption("Sistema de Monitoramento de Qualidade de Dados | Visualização  v1.0 ")