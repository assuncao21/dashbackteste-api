import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, HTTPException

router = APIRouter()


def get_connection():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise HTTPException(status_code=500, detail="DATABASE_URL não configurada")
    return psycopg2.connect(url)


@router.post("/analizar-contextos")
def analizar_contextos(data: dict):
    id_setup_grupo = data.get("id_setup_grupo")
    if not id_setup_grupo:
        raise HTTPException(status_code=400, detail="id_setup_grupo obrigatório")

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT
                ativo,
                COUNT(*) AS total,
                SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END) AS wins,
                ROUND(
                    100.0 * SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                ) AS win_rate,
                COALESCE(SUM(resultado_pips), 0) AS resultado_pips_total,
                ROUND(AVG(resultado_pips), 2) AS expectativa_pips,
                ROUND(
                    COALESCE(
                        SUM(CASE WHEN resultado_pips > 0 THEN resultado_pips ELSE 0 END)
                        / NULLIF(ABS(SUM(CASE WHEN resultado_pips < 0 THEN resultado_pips ELSE 0 END)), 0),
                        0
                    ), 2
                ) AS profit_factor
            FROM operacoes
            WHERE id_setup_grupo = %s
            GROUP BY ativo
            ORDER BY win_rate DESC NULLS LAST
        """, (id_setup_grupo,))
        por_ativo = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT
                hora_entrada,
                COUNT(*) AS total,
                SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END) AS wins,
                ROUND(
                    100.0 * SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                ) AS win_rate,
                COALESCE(SUM(resultado_pips), 0) AS resultado_pips_total,
                ROUND(AVG(resultado_pips), 2) AS expectativa_pips
            FROM operacoes
            WHERE id_setup_grupo = %s AND hora_entrada IS NOT NULL
            GROUP BY hora_entrada
            ORDER BY hora_entrada
        """, (id_setup_grupo,))
        por_hora = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT
                dia_semana,
                COUNT(*) AS total,
                SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END) AS wins,
                ROUND(
                    100.0 * SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                ) AS win_rate,
                COALESCE(SUM(resultado_pips), 0) AS resultado_pips_total,
                ROUND(AVG(resultado_pips), 2) AS expectativa_pips
            FROM operacoes
            WHERE id_setup_grupo = %s AND dia_semana IS NOT NULL
            GROUP BY dia_semana
            ORDER BY dia_semana
        """, (id_setup_grupo,))
        por_dia_semana = [dict(r) for r in cur.fetchall()]

        # Top 10 combinações ativo + hora + dia com mínimo 5 ops
        cur.execute("""
            SELECT
                ativo,
                hora_entrada,
                dia_semana,
                COUNT(*) AS total,
                SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END) AS wins,
                ROUND(
                    100.0 * SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                ) AS win_rate,
                COALESCE(SUM(resultado_pips), 0) AS resultado_pips_total,
                ROUND(AVG(resultado_pips), 2) AS expectativa_pips,
                ROUND(
                    COALESCE(
                        SUM(CASE WHEN resultado_pips > 0 THEN resultado_pips ELSE 0 END)
                        / NULLIF(ABS(SUM(CASE WHEN resultado_pips < 0 THEN resultado_pips ELSE 0 END)), 0),
                        0
                    ), 2
                ) AS profit_factor
            FROM operacoes
            WHERE id_setup_grupo = %s
            GROUP BY ativo, hora_entrada, dia_semana
            HAVING COUNT(*) >= 5
            ORDER BY win_rate DESC NULLS LAST, total DESC
            LIMIT 10
        """, (id_setup_grupo,))
        melhores_contextos = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT
                COUNT(*) AS total,
                ROUND(
                    100.0 * SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                ) AS win_rate_global
            FROM operacoes
            WHERE id_setup_grupo = %s
        """, (id_setup_grupo,))
        stats = cur.fetchone()

        cur.close()
        conn.close()

        total_ops = int(stats["total"]) if stats else 0
        win_rate_global = float(stats["win_rate_global"] or 0) if stats else 0.0

        score_ops = min(total_ops ** 0.5 * 4, 100)
        score_confiabilidade = round(score_ops * 0.3 + win_rate_global * 0.7, 1)

        melhor_contexto = melhores_contextos[0] if melhores_contextos else None

        return {
            "id_setup_grupo": id_setup_grupo,
            "por_ativo": por_ativo,
            "por_hora": por_hora,
            "por_dia_semana": por_dia_semana,
            "melhores_contextos": melhores_contextos,
            "melhor_contexto": melhor_contexto,
            "score_confiabilidade": score_confiabilidade,
            "total_operacoes": total_ops,
            "win_rate_global": win_rate_global,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/contextos-otimizados")
def contextos_otimizados(data: dict):
    """Retorna contextos filtrados para uso no EA operacional."""
    id_setup_grupo = data.get("id_setup_grupo")
    win_rate_minimo = float(data.get("win_rate_minimo", 55))
    min_operacoes = int(data.get("min_operacoes", 5))

    if not id_setup_grupo:
        raise HTTPException(status_code=400, detail="id_setup_grupo obrigatório")

    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT
                ativo,
                hora_entrada,
                dia_semana,
                COUNT(*) AS total,
                ROUND(
                    100.0 * SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                ) AS win_rate,
                ROUND(AVG(resultado_pips), 2) AS expectativa_pips,
                ROUND(
                    COALESCE(
                        SUM(CASE WHEN resultado_pips > 0 THEN resultado_pips ELSE 0 END)
                        / NULLIF(ABS(SUM(CASE WHEN resultado_pips < 0 THEN resultado_pips ELSE 0 END)), 0),
                        0
                    ), 2
                ) AS profit_factor
            FROM operacoes
            WHERE id_setup_grupo = %s
            GROUP BY ativo, hora_entrada, dia_semana
            HAVING
                COUNT(*) >= %s
                AND (
                    100.0 * SUM(CASE WHEN status_operacao = 'WIN' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0)
                ) >= %s
            ORDER BY win_rate DESC, total DESC
        """, (id_setup_grupo, min_operacoes, win_rate_minimo))

        contextos = [dict(r) for r in cur.fetchall()]

        cur.close()
        conn.close()

        return {
            "id_setup_grupo": id_setup_grupo,
            "filtros_aplicados": {
                "win_rate_minimo": win_rate_minimo,
                "min_operacoes": min_operacoes,
            },
            "total_contextos": len(contextos),
            "contextos": contextos,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
