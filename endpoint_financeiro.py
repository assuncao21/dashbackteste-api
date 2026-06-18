import os
from datetime import datetime
from typing import List, Optional

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


def _parse_dt(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value[:19])
        except Exception:
            return None
    return None


def _calcular_bloco_financeiro(rows: list, banca_inicial: float, valor_pip: float) -> dict:
    trades = [float(r["resultado_pips"] or 0) for r in rows]
    if not trades:
        return {}

    def p2f(pips: float) -> float:
        return round(pips * valor_pip, 2)

    curva_pips, dd_pips = scoring.calcular_curva_e_drawdown(trades)
    resultado_pips = sum(trades)
    resultado_fin = p2f(resultado_pips)
    dd_fin = p2f(dd_pips)

    curva_fin = [round(banca_inicial + p2f(p), 2) for p in curva_pips]

    # Drawdown em moeda por ponto da curva
    curva_dd_fin = []
    pico_pips = 0.0
    for p in curva_pips:
        if p > pico_pips:
            pico_pips = p
        curva_dd_fin.append(round(-p2f(pico_pips - p), 2))

    total = len(trades)
    exp_pip = resultado_pips / total if total > 0 else 0.0
    exp_fin = p2f(exp_pip)
    recovery = round(resultado_fin / dd_fin, 2) if dd_fin != 0 else 0.0
    retorno_pct = round((resultado_fin / banca_inicial) * 100.0, 2) if banca_inicial > 0 else 0.0

    # Estimativas mensais / anuais
    dts = [_parse_dt(r.get("data_hora_entrada")) for r in rows]
    dts = [d for d in dts if d]
    if len(dts) >= 2:
        delta_dias = (max(dts) - min(dts)).days
        meses = max(1.0, delta_dias / 30.0)
        exp_mensal = round(resultado_fin / meses, 2)
        exp_anual = round(exp_mensal * 12.0, 2)
    else:
        exp_mensal = exp_fin
        exp_anual = round(exp_fin * 12.0, 2)

    return {
        "total_operacoes": total,
        "resultado_pips": round(resultado_pips, 2),
        "lucro_total": resultado_fin,
        "banca_final": round(banca_inicial + resultado_fin, 2),
        "drawdown_financeiro": dd_fin,
        "recovery": recovery,
        "retorno_percentual": retorno_pct,
        "expectativa_por_trade": round(exp_fin, 2),
        "expectativa_mensal": exp_mensal,
        "expectativa_anual": exp_anual,
        "curva_financeira": curva_fin,
        "curva_drawdown_financeiro": curva_dd_fin,
    }


@router.post("/resultados-financeiros")
def resultados_financeiros(data: dict):
    id_setup_grupo = data.get("id_setup_grupo")
    if not id_setup_grupo:
        raise HTTPException(400, "id_setup_grupo obrigatório")

    banca_inicial = float(data.get("banca_inicial", 10000.0))
    valor_pip = float(data.get("valor_pip", 10.0))
    dhas_aprovados = data.get("dhas_aprovados")  # lista de {ativo, hora, dia_semana}

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT resultado_pips, status_operacao, data_hora_entrada,
                   ativo, hora_entrada, dia_semana
            FROM operacoes
            WHERE id_setup_grupo = %s
            ORDER BY data_hora_entrada, id_operacao
            """,
            (id_setup_grupo,),
        )
        all_rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()

        if not all_rows:
            raise HTTPException(404, "Nenhuma operação encontrada")

        resultado_bruto = _calcular_bloco_financeiro(all_rows, banca_inicial, valor_pip)
        resultado_bruto["label"] = "Bruto"

        resultado_otimizado = None
        if dhas_aprovados:
            aprovados_set = {
                (str(d["ativo"]), int(d["hora"]), int(d["dia_semana"]))
                for d in dhas_aprovados
            }
            rows_otim = [
                r for r in all_rows
                if (
                    str(r.get("ativo") or ""),
                    int(r.get("hora_entrada") or 0),
                    int(r.get("dia_semana") or 0),
                ) in aprovados_set
            ]
            if rows_otim:
                resultado_otimizado = _calcular_bloco_financeiro(rows_otim, banca_inicial, valor_pip)
                resultado_otimizado["label"] = "Otimizado"

        return {
            "id_setup_grupo": id_setup_grupo,
            "banca_inicial": banca_inicial,
            "valor_pip": valor_pip,
            "bruto": resultado_bruto,
            "otimizado": resultado_otimizado,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))