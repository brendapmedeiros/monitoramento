import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Dict


def create_quality_score_timeline(df: pd.DataFrame) -> go.Figure:
    """Gráfico de linha do qs ao longo do tempo"""
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['quality_score'],
        mode='lines+markers',
        name='Quality Score',
        line=dict(color='#2E86AB', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(46, 134, 171, 0.1)'
    ))
    
    # Linha de threshold
    fig.add_hline(
        y=85, 
        line_dash="dash", 
        line_color="orange",
        annotation_text="Threshold (85%)"
    )
    
    fig.update_layout(
        title="Quality Score ao Longo do Tempo",
        xaxis_title="Data",
        yaxis_title="Quality Score (%)",
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return fig


def create_metrics_breakdown(df: pd.DataFrame) -> go.Figure:
    """Gráfico de barras com breakdown das métricas"""
    
    latest = df.iloc[-1]
    
    metrics = {
        'Completude': latest['completeness'],
        'Unicidade': latest['uniqueness'],
        'Validade': latest['validity'],
        'Consistência': latest['consistency']
    }
    
    colors = ['#06D6A0', '#118AB2', '#073B4C', '#FFD166']
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=list(metrics.keys()),
        y=list(metrics.values()),
        marker_color=colors,
        text=[f"{v:.2f}%" for v in metrics.values()],
        textposition='auto',
    ))
    
    fig.update_layout(
        title="Breakdown de Métricas de Qualidade (Última Execução)",
        yaxis_title="Pontuação (%)",
        template='plotly_white',
        height=400,
        showlegend=False
    )
    
    fig.update_yaxes(range=[0, 100])
    
    return fig


def create_anomalies_by_method(df: pd.DataFrame) -> go.Figure:
    """Gráfico de barras empilhadas - anomalias por método"""
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Z-Score',
        x=df['timestamp'],
        y=df['zscore'],
        marker_color='#EF476F'
    ))
    
    fig.add_trace(go.Bar(
        name='IQR',
        x=df['timestamp'],
        y=df['iqr'],
        marker_color='#FFD166'
    ))
    
    fig.add_trace(go.Bar(
        name='Isolation Forest',
        x=df['timestamp'],
        y=df['isolation_forest'],
        marker_color='#06D6A0'
    ))
    
    fig.update_layout(
        title="Anomalias Detectadas por Método",
        xaxis_title="Data",
        yaxis_title="Quantidade de Anomalias",
        barmode='stack',
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return fig


def create_severity_pie(df: pd.DataFrame) -> go.Figure:
    """Gráfico de pizza - distribuição de severidade"""
    
    latest = df.iloc[-1]
    
    labels = ['Alta', 'Média', 'Baixa']
    values = [
        latest['severity_high'],
        latest['severity_medium'],
        latest['severity_low']
    ]
    colors = ['#EF476F', '#FFD166', '#06D6A0']
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        marker_colors=colors,
        hole=0.4,
        textinfo='label+percent',
        textfont_size=14
    )])
    
    fig.update_layout(
        title="Distribuição de Severidade das Anomalias",
        template='plotly_white',
        height=400
    )
    
    return fig


def create_metrics_heatmap(df: pd.DataFrame) -> go.Figure:
    """Heatmap das métricas ao longo do tempo"""
    
    metrics_cols = ['completeness', 'uniqueness', 'validity', 'consistency']
    
    data = df[metrics_cols].T
    
    fig = go.Figure(data=go.Heatmap(
        z=data.values,
        x=df['timestamp'].dt.strftime('%Y-%m-%d %H:%M'),
        y=['Completude', 'Unicidade', 'Validade', 'Consistência'],
        colorscale='RdYlGn',
        zmin=0,
        zmax=100,
        text=data.values,
        texttemplate='%{text:.1f}%',
        textfont={"size": 10},
        colorbar=dict(title="Score (%)")
    ))
    
    fig.update_layout(
        title="Heatmap de Métricas de Qualidade",
        xaxis_title="Data",
        template='plotly_white',
        height=400
    )
    
    return fig


def create_anomaly_trend(df: pd.DataFrame) -> go.Figure:
    """Gráfico de tendência de anomalias"""
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['anomaly_percentage'],
        mode='lines+markers',
        name='% de Anomalias',
        line=dict(color='#EF476F', width=2),
        marker=dict(size=8)
    ))
    
    # Threshold de 5%
    fig.add_hline(
        y=5.0,
        line_dash="dash",
        line_color="orange",
        annotation_text="Threshold (5%)"
    )
    
    fig.update_layout(
        title="Tendência de Anomalias ao Longo do Tempo",
        xaxis_title="Data",
        yaxis_title="Percentual de Anomalias (%)",
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return fig
