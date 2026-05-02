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
                barras_para_varrer
            )
            VALUES (
                %(IDSetup)s,
                %(IDOperacao)s,
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
                %(BarrasParaVarrer)s
            )
        """, data)

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "ok",
            "mensagem": "Operação registrada com sucesso",
            "id_operacao": data.get("IDOperacao")
        }

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
