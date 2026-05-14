import os
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL não configurada")
    return psycopg2.connect(DATABASE_URL)


@app.get("/")
def home():
    return {"status": "API DashBackTeste online"}


@app.post("/operacoes")
def receber_operacao(data: dict):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO operacoes (
                id_setup,
                id_operacao,
                id_setup_grupo,
                nome_setup,
                id_lote_importacao,
                parametros_setup,
                versao_ea,
                data_hora_entrada,
                data_hora_saida,
                utc_local,
                utc_corretora,
                ativo,
                timeframe,
                tipo_ordem,
                lote,
                preco_entrada,
                preco_saida,
                stop_loss,
                take_profit,
                spread,
                spread_historico_entrada,
                spread_historico_saida,
                comissao_por_lote_round_turn,
                comissao,
                resultado_pips,
                max_favoravel_pips,
                max_contra_pips,
                ganho_nao_concluido_por_spread,
                resultado_bruto,
                resultado_financeiro,
                saldo_antes,
                saldo_depois,
                estrategia,
                indicador,
                hora_entrada,
                dia_semana,
                status_operacao,
                tipo_de_fechamento,
                sma_filtro,
                distancia_preco_sma,
                rsi_valor,
                adx_valor,
                volume_valor,
                filtro_sma_status,
                filtro_rsi_status,
                filtro_adx_status,
                filtro_volume_status,
                filtros_aprovados,
                modo_entrada,
                stop_loss_pontos,
                take_profit_pontos,
                data_referencia,
                hora_referencia,
                barras_para_varrer,
                usar_filtro_sma,
                periodo_sma_filtro,
                usar_filtro_rsi,
                periodo_rsi,
                rsi_compra_min,
                rsi_venda_max,
                usar_filtro_adx,
                periodo_adx,
                adx_minimo,
                usar_filtro_volume,
                volume_minimo
            )
            VALUES (
                %(IDSetup)s,
                %(IDOperacao)s,
                %(IDSetupGrupo)s,
                %(NomeSetup)s,
                %(IDLoteImportacao)s,
                %(ParametrosSetup)s,
                %(VersaoEA)s,
                %(DataHoraEntrada)s,
                %(DataHoraSaida)s,
                %(UTCLocal)s,
                %(UTCCorretora)s,
                %(Ativo)s,
                %(Timeframe)s,
                %(TipoOrdem)s,
                %(Lote)s,
                %(PrecoEntrada)s,
                %(PrecoSaida)s,
                %(StopLoss)s,
                %(TakeProfit)s,
                %(Spread)s,
                %(SpreadHistoricoEntrada)s,
                %(SpreadHistoricoSaida)s,
                %(ComissaoPorLoteRoundTurn)s,
                %(Comissao)s,
                %(ResultadoPips)s,
                %(MaxFavoravelPips)s,
                %(MaxContraPips)s,
                %(GanhoNaoConcluidoPorSpread)s,
                %(ResultadoBruto)s,
                %(ResultadoFinanceiro)s,
                %(SaldoAntes)s,
                %(SaldoDepois)s,
                %(Estrategia)s,
                %(Indicador)s,
                %(HoraEntrada)s,
                %(DiaSemana)s,
                %(StatusOperacao)s,
                %(TipoDeFechamento)s,
                %(SMAFiltro)s,
                %(DistanciaPrecoSMA)s,
                %(RSIValor)s,
                %(ADXValor)s,
                %(VolumeValor)s,
                %(FiltroSMAStatus)s,
                %(FiltroRSIStatus)s,
                %(FiltroADXStatus)s,
                %(FiltroVolumeStatus)s,
                %(FiltrosAprovados)s,
                %(ModoEntrada)s,
                %(StopLossPontos)s,
                %(TakeProfitPontos)s,
                %(DataReferencia)s,
                %(HoraReferencia)s,
                %(BarrasParaVarrer)s,
                %(UsarFiltroSMA)s,
                %(PeriodoSMAFiltro)s,
                %(UsarFiltroRSI)s,
                %(PeriodoRSI)s,
                %(RSICompraMin)s,
                %(RSIVendaMax)s,
                %(UsarFiltroADX)s,
                %(PeriodoADX)s,
                %(ADXMinimo)s,
                %(UsarFiltroVolume)s,
                %(VolumeMinimo)s
            )
            ON CONFLICT (id_operacao)
            DO UPDATE SET
                id_setup = EXCLUDED.id_setup,
                id_setup_grupo = EXCLUDED.id_setup_grupo,
                nome_setup = EXCLUDED.nome_setup,
                id_lote_importacao = EXCLUDED.id_lote_importacao,
                parametros_setup = EXCLUDED.parametros_setup,
                versao_ea = EXCLUDED.versao_ea,
                data_hora_entrada = EXCLUDED.data_hora_entrada,
                data_hora_saida = EXCLUDED.data_hora_saida,
                utc_local = EXCLUDED.utc_local,
                utc_corretora = EXCLUDED.utc_corretora,
                ativo = EXCLUDED.ativo,
                timeframe = EXCLUDED.timeframe,
                tipo_ordem = EXCLUDED.tipo_ordem,
                lote = EXCLUDED.lote,
                preco_entrada = EXCLUDED.preco_entrada,
                preco_saida = EXCLUDED.preco_saida,
                stop_loss = EXCLUDED.stop_loss,
                take_profit = EXCLUDED.take_profit,
                spread = EXCLUDED.spread,
                spread_historico_entrada = EXCLUDED.spread_historico_entrada,
                spread_historico_saida = EXCLUDED.spread_historico_saida,
                comissao_por_lote_round_turn = EXCLUDED.comissao_por_lote_round_turn,
                comissao = EXCLUDED.comissao,
                resultado_pips = EXCLUDED.resultado_pips,
                max_favoravel_pips = EXCLUDED.max_favoravel_pips,
                max_contra_pips = EXCLUDED.max_contra_pips,
                ganho_nao_concluido_por_spread = EXCLUDED.ganho_nao_concluido_por_spread,
                resultado_bruto = EXCLUDED.resultado_bruto,
                resultado_financeiro = EXCLUDED.resultado_financeiro,
                saldo_antes = EXCLUDED.saldo_antes,
                saldo_depois = EXCLUDED.saldo_depois,
                estrategia = EXCLUDED.estrategia,
                indicador = EXCLUDED.indicador,
                hora_entrada = EXCLUDED.hora_entrada,
                dia_semana = EXCLUDED.dia_semana,
                status_operacao = EXCLUDED.status_operacao,
                tipo_de_fechamento = EXCLUDED.tipo_de_fechamento,
                sma_filtro = EXCLUDED.sma_filtro,
                distancia_preco_sma = EXCLUDED.distancia_preco_sma,
                rsi_valor = EXCLUDED.rsi_valor,
                adx_valor = EXCLUDED.adx_valor,
                volume_valor = EXCLUDED.volume_valor,
                filtro_sma_status = EXCLUDED.filtro_sma_status,
                filtro_rsi_status = EXCLUDED.filtro_rsi_status,
                filtro_adx_status = EXCLUDED.filtro_adx_status,
                filtro_volume_status = EXCLUDED.filtro_volume_status,
                filtros_aprovados = EXCLUDED.filtros_aprovados,
                modo_entrada = EXCLUDED.modo_entrada,
                stop_loss_pontos = EXCLUDED.stop_loss_pontos,
                take_profit_pontos = EXCLUDED.take_profit_pontos,
                data_referencia = EXCLUDED.data_referencia,
                hora_referencia = EXCLUDED.hora_referencia,
                barras_para_varrer = EXCLUDED.barras_para_varrer,
                usar_filtro_sma = EXCLUDED.usar_filtro_sma,
                periodo_sma_filtro = EXCLUDED.periodo_sma_filtro,
                usar_filtro_rsi = EXCLUDED.usar_filtro_rsi,
                periodo_rsi = EXCLUDED.periodo_rsi,
                rsi_compra_min = EXCLUDED.rsi_compra_min,
                rsi_venda_max = EXCLUDED.rsi_venda_max,
                usar_filtro_adx = EXCLUDED.usar_filtro_adx,
                periodo_adx = EXCLUDED.periodo_adx,
                adx_minimo = EXCLUDED.adx_minimo,
                usar_filtro_volume = EXCLUDED.usar_filtro_volume,
                volume_minimo = EXCLUDED.volume_minimo,
                criado_em = now()
        """, data)

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "ok",
            "mensagem": "Operação registrada/atualizada com sucesso",
            "id_operacao": data.get("IDOperacao")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/simular")
def simular(data: dict):

    return {
        "resumo": {
            "score": 74,
            "win_rate": "61%",
            "drawdown": "12%",
            "total_operacoes": 87
        },
        "curva_capital": [
            1000,
            1012,
            1005,
            1020,
            1045
        ],
        "alertas": [
            "Amostra moderada",
            "Drawdown aceitável"
        ]
    }

@app.get("/setups")
def listar_setups():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
    WITH setups AS (
        SELECT
            id_setup_grupo,
            MAX(nome_setup) AS nome_setup,
            MAX(estrategia) AS estrategia,
            MAX(timeframe) AS timeframe,
            MAX(indicador) AS indicador,

            MAX(utc_local) AS utc_local,
            MAX(utc_corretora) AS utc_corretora,

            MAX(lote) AS lote,
            MAX(stop_loss_pontos) AS stop_loss_pontos,
            MAX(take_profit_pontos) AS take_profit_pontos,
            MAX(comissao_por_lote_round_turn) AS comissao_por_lote_round_turn,

            STRING_AGG(DISTINCT ativo, ', ' ORDER BY ativo) AS ativos,
            COUNT(*) AS total_operacoes,
            MIN(data_hora_entrada) AS primeira_operacao,
            MAX(data_hora_saida) AS ultima_operacao,
            MAX(parametros_setup) AS parametros_setup
        FROM operacoes
        WHERE id_setup_grupo IS NOT NULL
        GROUP BY id_setup_grupo
    ),

    coletas_base AS (
        SELECT
            id_setup_grupo,
            id_lote_importacao,
            MIN(criado_em) AS data_execucao_coleta,
            MAX(data_referencia) AS data_referencia,
            MAX(hora_referencia) AS hora_referencia,
            MAX(barras_para_varrer) AS barras_para_varrer,
            MIN(data_hora_entrada) AS primeira_operacao_coleta,
            MAX(data_hora_saida) AS ultima_operacao_coleta,
            COUNT(*) AS total_operacoes_coleta,
            MAX(utc_local) AS utc_local,
            MAX(utc_corretora) AS utc_corretora
        FROM operacoes
        WHERE id_setup_grupo IS NOT NULL
        GROUP BY id_setup_grupo, id_lote_importacao
    ),

    coletas AS (
        SELECT
            id_setup_grupo,
            JSON_AGG(
                JSON_BUILD_OBJECT(
                    'id_lote_importacao', id_lote_importacao,
                    'data_execucao_coleta', data_execucao_coleta,
                    'data_referencia', data_referencia,
                    'hora_referencia', hora_referencia,
                    'barras_para_varrer', barras_para_varrer,
                    'primeira_operacao', primeira_operacao_coleta,
                    'ultima_operacao', ultima_operacao_coleta,
                    'total_operacoes', total_operacoes_coleta,
                    'utc_local', utc_local,
                    'utc_corretora', utc_corretora
                )
                ORDER BY data_referencia DESC, hora_referencia DESC
            ) AS coletas
        FROM coletas_base
        GROUP BY id_setup_grupo
    )

    SELECT
        s.*,
        c.coletas
    FROM setups s
    LEFT JOIN coletas c
        ON s.id_setup_grupo = c.id_setup_grupo
    ORDER BY s.ultima_operacao DESC
""")

        dados = cur.fetchall()

        cur.close()
        conn.close()

        return dados

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/operacoes")
def listar_operacoes(limit: int = 100):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT *
            FROM operacoes
            ORDER BY id DESC
            LIMIT %s
        """, (limit,))

        dados = cur.fetchall()

        cur.close()
        conn.close()

        return dados

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
