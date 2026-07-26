-- ============================================================================
-- CASE TÉCNICO SENIOR: CONSULTAS SQL AVANÇADAS (DATABRICKS / UNITY CATALOG)
-- Autor: Steffan Sevalt
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: Média Móvel de Transações (Janela Móvel de 3 Meses)
-- ----------------------------------------------------------------------------
WITH MapeamentoMensal AS (
    SELECT 
        id_cliente,
        DATE_TRUNC('month', data_transacao) AS mes,
        SUM(valor_transacao) AS total_mensal
    FROM lakehouse_dev.gold.fact_transacoes
    GROUP BY id_cliente, DATE_TRUNC('month', data_transacao)
)
SELECT 
    id_cliente,
    mes,
    total_mensal,
    AVG(total_mensal) OVER (
        PARTITION BY id_cliente 
        ORDER BY mes 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS media_movel_3_meses
FROM MapeamentoMensal;

-- ----------------------------------------------------------------------------
-- QUERY 2: Detecção de Anomalias (Transações 3x acima da média do cliente)
-- ----------------------------------------------------------------------------
WITH EstatisticasCliente AS (
    SELECT 
        id_cliente,
        AVG(valor_transacao) AS media_historica,
        STDDEV(valor_transacao) AS desvio_padrao
    FROM lakehouse_dev.gold.fact_transacoes
    GROUP BY id_cliente
)
SELECT 
    t.id_transacao,
    t.id_cliente,
    t.data_transacao,
    t.valor_transacao,
    e.media_historica,
    ROUND(t.valor_transacao / e.media_historica, 2) AS fator_desvio
FROM lakehouse_dev.gold.fact_transacoes t
JOIN EstatisticasCliente e ON t.id_cliente = e.id_cliente
WHERE t.valor_transacao > (e.media_historica * 3)
ORDER BY fator_desvio DESC;

-- ----------------------------------------------------------------------------
-- QUERY 3: Tempo Decorrido entre Transações Consecutivas (Uso de LAG)
-- ----------------------------------------------------------------------------
WITH TransacoesComLag AS (
    SELECT 
        id_transacao,
        id_cliente,
        data_transacao,
        LAG(data_transacao) OVER (
            PARTITION BY id_cliente 
            ORDER BY data_transacao
        ) AS data_transacao_anterior
    FROM lakehouse_dev.gold.fact_transacoes
)
SELECT 
    id_transacao,
    id_cliente,
    data_transacao,
    data_transacao_anterior,
    ROUND((CAST(data_transacao AS LONG) - CAST(data_transacao_anterior AS LONG)) / 60, 2) AS minutos_desde_ultima_transacao
FROM TransacoesComLag
WHERE data_transacao_anterior IS NOT NULL;

-- ----------------------------------------------------------------------------
-- QUERY 4: Ranking de Clientes por Segmento (DENSE_RANK)
-- ----------------------------------------------------------------------------
WITH VolumetriaCliente AS (
    SELECT 
        c.segmento_cliente,
        t.id_cliente,
        c.nome_cliente,
        SUM(t.valor_transacao) AS volume_total
    FROM lakehouse_dev.gold.fact_transacoes t
    JOIN lakehouse_dev.silver.clientes c ON t.id_cliente = c.id_cliente AND c._registro_atual = true
    GROUP BY c.segmento_cliente, t.id_cliente, c.nome_cliente
)
SELECT 
    segmento_cliente,
    id_cliente,
    nome_cliente,
    volume_total,
    DENSE_RANK() OVER (
        PARTITION BY segmento_cliente 
        ORDER BY volume_total DESC
    ) AS posicao_ranking
FROM VolumetriaCliente;

-- ----------------------------------------------------------------------------
-- QUERY 5: Identificação de Clientes Inativos (Sem transação nos últimos 90 dias)
-- ----------------------------------------------------------------------------
SELECT 
    c.id_cliente,
    c.nome_cliente,
    MAX(t.data_transacao) AS ultima_transacao,
    DATEDIFF(CURRENT_DATE(), MAX(t.data_transacao)) AS dias_sem_transacionar
FROM lakehouse_dev.silver.clientes c
LEFT JOIN lakehouse_dev.gold.fact_transacoes t ON c.id_cliente = t.id_cliente
WHERE c._registro_atual = true
GROUP BY c.id_cliente, c.nome_cliente
HAVING dias_sem_transacionar > 90 OR ultima_transacao IS NULL;

-- ----------------------------------------------------------------------------
-- QUERY 6: Reconstrução de Saldo Acumulado por Conta (SUM OVER UNBOUNDED PRECEDING)
-- ----------------------------------------------------------------------------
SELECT 
    id_conta,
    id_transacao,
    data_transacao,
    tipo_transacao,
    valor_transacao,
    SUM(CASE WHEN tipo_transacao = 'CREDITO' THEN valor_transacao ELSE -valor_transacao END) OVER (
        PARTITION BY id_conta 
        ORDER BY data_transacao 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS saldo_reconstruido_momento
FROM lakehouse_dev.gold.fact_transacoes;

-- ----------------------------------------------------------------------------
-- QUERY 7: Consolidação Executiva Mensal de Risco e Compliance
-- ----------------------------------------------------------------------------
SELECT 
    DATE_TRUNC('month', data_transacao) AS mes_referencia,
    segmento_cliente,
    COUNT(DISTINCT id_transacao) AS total_transacoes,
    COUNT(DISTINCT id_cliente) AS total_clientes_ativos,
    SUM(valor_transacao) AS volume_financeiro_total,
    AVG(valor_transacao) AS ticket_medio,
    COUNT(CASE WHEN valor_transacao > 50000 THEN 1 END) AS qtd_transacoes_alta_alerta
FROM lakehouse_dev.gold.fact_transacoes
GROUP BY DATE_TRUNC('month', data_transacao), segmento_cliente
ORDER BY mes_referencia DESC, volume_financeiro_total DESC;
