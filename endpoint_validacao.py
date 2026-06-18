import os
from typing import Optional

import psycopg2
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor

import scoring

router = APIRouter()


def get_connection():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise HTTPException(500, "DATABASE_URL não configurada")
    return psycopg2.connect(url)


def _fetch_trades(cur, id_setup_grupo: str):
    cur.execute(
        """
        SELECT resultado_pips, status_operacao, data_hora_entrada
        FROM operacoes
        WHERE id_setup_grupo = %s
        ORDER BY data_hora_entrada, id_operacao
        """,
        (id_setup_grupo,),
    )
    rows = cur.fetchall()
    trades = [float(r["resultado_pips"] or 0) for r in rows]
    statuses = [str(r["status_operacao"] or "") for r in rows]
    trades_com_data = [dict(r) for r in rows]
    return trades, statuses, trades_com_data


@router.post("/score-bruto")
def calcular_score_bruto(data: dict):
    id_setup_grupo = data.get("id_setup_grupo")
    if not id_setup_grupo:
        raise HTTPException(400, "id_setup_grupo obrigatório")
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        trades, statuses, trades_com_data = _fetch_trades(cur, id_setup_grupo)
        cur.close()
        conn.close()

        if not trades:
            raise HTTPException(404, "Nenhuma operação encontrada")

        resultado = scoring.calcular_score_bruto_completo(trades, statuses, trades_com_data)
        resultado["id_setup_grupo"] = id_setup_grupo
        return resultado

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/ranking-setups")
def ranking_setups(estrategia: Optional[str] = None):
    """
    Retorna todos os setups ranqueados por Score Bruto.
    Passa estrategia como query param para filtrar.
    """
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT
                id_setup_grupo,
                MAX(nome_setup)  AS nome_setup,
                MAX(estrategia)  AS estrategia,
                MAX(timeframe)   AS timeframe,
                COUNT(*)         AS total_operacoes
            FROM operacoes
            WHERE id_setup_grupo IS NOT NULL
            GROUP BY id_setup_grupo
            ORDER BY total_operacoes DESC
            """
        )
        setups_raw = cur.fetchall()

        resultados = []
        for setup in setups_raw:
            if estrategia and setup["estrategia"] != estrategia:
                continue

            cur2 = conn.cursor(cursor_factory=RealDictCursor)
            trades, statuses, trades_com_data = _fetch_trades(cur2, setup["id_setup_grupo"])
            cur2.close()

            sb = scoring.calcular_score_bruto_completo(trades, statuses, trades_com_data)
            resultados.append(
                {
                    "id_setup_grupo": setup["id_setup_grupo"],
                    "nome_setup": setup["nome_setup"],
                    "estrategia": setup["estrategia"],
                    "timeframe": setup["timeframe"],
                    "total_operacoes": int(setup["total_operacoes"]),
                    "score_bruto": sb["score_bruto"],
                    "classificacao": sb["classificacao"],
                    "metricas": sb["metricas"],
                }
            )

        cur.close()
        conn.close()

        resultados.sort(key=lambda x: x["score_bruto"], reverse=True)
        for i, r in enumerate(resultados):
            r["ranking"] = i + 1

        return resultados

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))