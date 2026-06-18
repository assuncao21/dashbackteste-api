import os
import statistics
from datetime import datetime
from typing import Optional

import psycopg2
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor

import scoring

router = APIRouter()

DIAS_SEMANA = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"]


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


@router.post("/otimizar")
def otimizar(data: dict):
    id_setup_grupo = data.get("id_setup_grupo")
    if not id_setup_grupo:
        raise HTTPException(400, "id_setup_grupo obrigatório")

    # Critérios de aprovação DHA (parametrizáveis)
    score_dha_min = float(data.get("score_dha_min", 60.0))
    n_dha_min = int(data.get("n_dha_min", 20))
    o_dha_min = int(data.get("o_dha_min", 10))

    # Critérios de validação da otimização
    n_otimizado_min = int(data.get("n_otimizado_min", 100))
    retencao_min = float(data.get("retencao_min", 0.15))

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

        # ── Score Bruto ──────────────────────────────────────────────────────
        trades_bruto = [float(r["resultado_pips"] or 0) for r in all_rows]
        statuses_bruto = [str(r["status_operacao"] or "") for r in all_rows]

        score_bruto_result = scoring.calcular_score_bruto_completo(
            trades_bruto, statuses_bruto, all_rows
        )

        # ── Agrupar por DHA ──────────────────────────────────────────────────
        dha_groups: dict = {}
        for row in all_rows:
            chave = (
                str(row.get("ativo") or ""),
                int(row.get("hora_entrada") or 0),
                int(row.get("dia_semana") or 0),
            )
            dha_groups.setdefault(chave, []).append(row)

        # ── Calcular Score e IC para cada DHA ────────────────────────────────
        dha_results = []
        aprovados_set: set = set()

        for (ativo, hora, dia), group_rows in dha_groups.items():
            n_dha = len(group_rows)

            # Ocorrências distintas por data calendário
            trades_por_data: dict = {}
            lucros_por_data: dict = {}
            for r in group_rows:
                dt = _parse_dt(r.get("data_hora_entrada"))
                if dt:
                    dk = dt.date()
                    trades_por_data[dk] = trades_por_data.get(dk, 0) + 1
                    lucros_por_data[dk] = lucros_por_data.get(dk, 0.0) + float(r.get("resultado_pips") or 0)

            o_dha = len(trades_por_data)

            # CV_DHA
            counts = list(trades_por_data.values())
            if len(counts) > 1:
                mean_c = statistics.mean(counts)
                cv_dha = statistics.stdev(counts) / mean_c if mean_c > 0 else 1.0
            else:
                cv_dha = 1.0

            ct_dha = scoring.calcular_hhi(list(trades_por_data.values()))
            cl_dha = scoring.calcular_hhi(list(lucros_por_data.values()))

            ic_dha = scoring.calcular_ic_dha(n_dha, o_dha, cv_dha, ct_dha, cl_dha)

            group_trades = [float(r["resultado_pips"] or 0) for r in group_rows]
            group_statuses = [str(r["status_operacao"] or "") for r in group_rows]

            score_dha = scoring.calcular_score_dha(group_trades, group_statuses)
            score_dha_ajustado = round(score_dha * ic_dha, 2)

            pf = scoring.calcular_profit_factor(group_trades)
            exp = scoring.calcular_expectativa(group_trades)
            _, dd = scoring.calcular_curva_e_drawdown(group_trades)
            resultado = sum(group_trades)
            rf = round(resultado / dd, 2) if dd > 0 else 0.0
            wins = sum(1 for s in group_statuses if s == "WIN")
            win_rate = round(100.0 * wins / n_dha, 2) if n_dha > 0 else 0.0

            aprovado = (
                score_dha_ajustado >= score_dha_min
                and n_dha >= n_dha_min
                and o_dha >= o_dha_min
            )

            dha_results.append(
                {
                    "ativo": ativo,
                    "hora": hora,
                    "dia_semana": dia,
                    "dia_semana_nome": DIAS_SEMANA[dia] if 0 <= dia <= 6 else str(dia),
                    "n_dha": n_dha,
                    "o_dha": o_dha,
                    "cv_dha": round(cv_dha, 4),
                    "ic_dha": ic_dha,
                    "score_dha": score_dha,
                    "score_dha_ajustado": score_dha_ajustado,
                    "aprovado": aprovado,
                    "profit_factor": round(pf, 2),
                    "expectativa_pips": round(exp, 2),
                    "drawdown_pips": round(dd, 2),
                    "recovery_factor": rf,
                    "win_rate": win_rate,
                    "resultado_pips": round(resultado, 2),
                    "confiabilidade": round(ic_dha * 100.0, 1),
                }
            )

            if aprovado:
                aprovados_set.add((ativo, hora, dia))

        # ── Trades otimizados ────────────────────────────────────────────────
        rows_otim = [
            r for r in all_rows
            if (
                str(r.get("ativo") or ""),
                int(r.get("hora_entrada") or 0),
                int(r.get("dia_semana") or 0),
            ) in aprovados_set
        ]

        n_bruto = len(all_rows)
        n_otimizado = len(rows_otim)
        retencao = round(n_otimizado / n_bruto, 4) if n_bruto > 0 else 0.0
        otimizacao_valida = n_otimizado >= n_otimizado_min and retencao >= retencao_min

        # ── Score Otimizado ──────────────────────────────────────────────────
        score_otimizado_result = None
        confiabilidade_otimizacao = None
        score_ranking = None

        if rows_otim:
            trades_otim = [float(r["resultado_pips"] or 0) for r in rows_otim]
            statuses_otim = [str(r["status_operacao"] or "") for r in rows_otim]

            score_otimizado_result = scoring.calcular_score_bruto_completo(
                trades_otim, statuses_otim, rows_otim
            )

            ganho = score_otimizado_result["score_bruto"] - score_bruto_result["score_bruto"]
            confiabilidade_otimizacao = round(ganho * retencao, 4)

            score_ranking = round(
                0.50 * score_otimizado_result["score_bruto"]
                + 0.30 * max(0.0, confiabilidade_otimizacao)
                + 0.20 * score_bruto_result["score_bruto"],
                2,
            )

        # ── Grid DHA (hora → dia → [ativos]) ────────────────────────────────
        dha_grid: dict = {}
        for dha in dha_results:
            if dha["aprovado"]:
                h = str(dha["hora"])
                d = str(dha["dia_semana"])
                dha_grid.setdefault(h, {}).setdefault(d, []).append(dha["ativo"])

        dha_results.sort(key=lambda x: x["score_dha_ajustado"], reverse=True)

        return {
            "id_setup_grupo": id_setup_grupo,
            "n_bruto": n_bruto,
            "n_otimizado": n_otimizado,
            "retencao": retencao,
            "otimizacao_valida": otimizacao_valida,
            "score_bruto": score_bruto_result,
            "score_otimizado": score_otimizado_result,
            "confiabilidade_otimizacao": confiabilidade_otimizacao,
            "score_ranking": score_ranking,
            "dhas": dha_results,
            "dha_aprovados": [
                {"ativo": d["ativo"], "hora": d["hora"], "dia_semana": d["dia_semana"]}
                for d in dha_results if d["aprovado"]
            ],
            "dha_grid": dha_grid,
            "parametros": {
                "score_dha_min": score_dha_min,
                "n_dha_min": n_dha_min,
                "o_dha_min": o_dha_min,
                "n_otimizado_min": n_otimizado_min,
                "retencao_min": retencao_min,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))