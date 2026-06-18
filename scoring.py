"""
Motor de scoring do DashBackTeste – Plano Básico.
Funções puras: sem acesso a banco de dados.
"""
import math
import random
import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple


# ─── helpers ──────────────────────────────────────────────────────────────────

def _parse_dt(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value[:19])
        except Exception:
            return None
    return None


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


# ─── métricas fundamentais ────────────────────────────────────────────────────

def calcular_profit_factor(trades: List[float]) -> float:
    ganhos = sum(t for t in trades if t > 0)
    perdas = abs(sum(t for t in trades if t < 0))
    return round(ganhos / perdas, 4) if perdas > 0 else 0.0


def calcular_expectativa(trades: List[float]) -> float:
    return round(statistics.mean(trades), 4) if trades else 0.0


def calcular_payoff(trades: List[float]) -> float:
    wins = [t for t in trades if t > 0]
    losses = [abs(t) for t in trades if t < 0]
    if not wins or not losses:
        return 0.0
    return round(statistics.mean(wins) / statistics.mean(losses), 4)


def calcular_curva_e_drawdown(trades: List[float]) -> Tuple[List[float], float]:
    curva = [0.0]
    saldo = 0.0
    pico = 0.0
    max_dd = 0.0
    for t in trades:
        saldo += t
        curva.append(round(saldo, 2))
        if saldo > pico:
            pico = saldo
        dd = pico - saldo
        if dd > max_dd:
            max_dd = dd
    return curva, round(max_dd, 4)


def calcular_curva_drawdown(trades: List[float]) -> List[float]:
    """Retorna drawdown em cada ponto (valores negativos)."""
    saldo = 0.0
    pico = 0.0
    curva = []
    for t in trades:
        saldo += t
        if saldo > pico:
            pico = saldo
        curva.append(round(-(pico - saldo), 2))
    return curva


def calcular_sequencia_max_perdas(statuses: List[str]) -> int:
    max_seq = seq = 0
    for s in statuses:
        if s == "LOSS":
            seq += 1
            max_seq = max(max_seq, seq)
        else:
            seq = 0
    return max_seq


def calcular_hhi(values: List[float]) -> float:
    """Índice Herfindahl-Hirschman (0 = uniforme, 1 = totalmente concentrado)."""
    total = sum(abs(v) for v in values)
    if total == 0:
        return 0.0
    return round(sum((abs(v) / total) ** 2 for v in values), 6)


# ─── métricas temporais ───────────────────────────────────────────────────────

def calcular_meses_positivos(trades_com_data: List[Dict]) -> float:
    meses: Dict[tuple, float] = {}
    for t in trades_com_data:
        dt = _parse_dt(t.get("data_hora_entrada"))
        if not dt:
            continue
        chave = (dt.year, dt.month)
        meses[chave] = meses.get(chave, 0.0) + float(t.get("resultado_pips") or 0)
    if not meses:
        return 0.5
    return round(sum(1 for v in meses.values() if v > 0) / len(meses), 4)


def calcular_semanas_positivas(trades_com_data: List[Dict]) -> float:
    semanas: Dict[tuple, float] = {}
    for t in trades_com_data:
        dt = _parse_dt(t.get("data_hora_entrada"))
        if not dt:
            continue
        chave = dt.isocalendar()[:2]
        semanas[chave] = semanas.get(chave, 0.0) + float(t.get("resultado_pips") or 0)
    if not semanas:
        return 0.5
    return round(sum(1 for v in semanas.values() if v > 0) / len(semanas), 4)


def calcular_distribuicao_lucro_score(trades: List[float]) -> float:
    """Score 0-100: quanto mais uniforme a distribuição dos ganhos, maior."""
    wins = [t for t in trades if t > 0]
    if len(wins) < 2:
        return 50.0
    hhi = calcular_hhi(wins)
    n = len(wins)
    hhi_min = 1.0 / n
    hhi_range = 1.0 - hhi_min
    if hhi_range <= 0:
        return 100.0
    normalized = (hhi - hhi_min) / hhi_range
    return round(_clamp((1.0 - normalized) * 100.0), 2)


def calcular_concentracao_temporal_score(trades_com_data: List[Dict]) -> float:
    """Score 0-100: quanto mais espalhadas no tempo as operações, maior."""
    meses: Dict[tuple, int] = {}
    lucros: Dict[tuple, float] = {}
    for t in trades_com_data:
        dt = _parse_dt(t.get("data_hora_entrada"))
        if not dt:
            continue
        chave = (dt.year, dt.month)
        meses[chave] = meses.get(chave, 0) + 1
        lucros[chave] = lucros.get(chave, 0.0) + float(t.get("resultado_pips") or 0)
    if not meses:
        return 50.0
    ct = calcular_hhi(list(meses.values()))
    cl = calcular_hhi(list(lucros.values()))
    concentracao = max(ct, cl)
    return round(_clamp((1.0 - concentracao) * 100.0), 2)


# ─── testes estatísticos ──────────────────────────────────────────────────────

def calcular_bootstrap(trades: List[float], n_iter: int = 500) -> float:
    """% de amostras bootstrap com Profit Factor > 1."""
    if len(trades) < 10:
        return 0.0
    positivos = sum(
        1 for _ in range(n_iter)
        if calcular_profit_factor(random.choices(trades, k=len(trades))) > 1.0
    )
    return round(100.0 * positivos / n_iter, 2)


def calcular_monte_carlo(trades: List[float], n_iter: int = 500) -> float:
    """% de caminhos aleatórios onde Recovery Factor >= 1."""
    if len(trades) < 10:
        return 0.0
    resultado_total = sum(trades)
    if resultado_total <= 0:
        return 0.0
    positivos = 0
    for _ in range(n_iter):
        path = random.sample(trades, len(trades))
        _, dd = calcular_curva_e_drawdown(path)
        if dd == 0 or resultado_total >= dd:
            positivos += 1
    return round(100.0 * positivos / n_iter, 2)


def calcular_out_of_sample(trades: List[float]) -> float:
    """Compara PF do in-sample (70%) com out-of-sample (30%)."""
    n = len(trades)
    if n < 20:
        return 0.0
    split = int(n * 0.7)
    is_pf = calcular_profit_factor(trades[:split])
    oos_pf = calcular_profit_factor(trades[split:])
    if is_pf <= 0:
        return 0.0
    return round(_clamp((oos_pf / is_pf) * 100.0), 2)


def calcular_walk_forward(trades: List[float], n_windows: int = 5) -> float:
    """Walk Forward: média do ratio OOS/IS em janelas deslizantes."""
    n = len(trades)
    if n < 30:
        return 0.0
    window_size = n // n_windows
    if window_size < 6:
        return 0.0
    is_size = int(window_size * 0.7)
    ratios = []
    for i in range(n_windows):
        start = i * window_size
        end = min(start + window_size, n)
        window = trades[start:end]
        if len(window) < 6:
            break
        is_pf = calcular_profit_factor(window[:is_size])
        oos_pf = calcular_profit_factor(window[is_size:])
        if is_pf > 0:
            ratios.append(oos_pf / is_pf)
    if not ratios:
        return 0.0
    return round(_clamp(statistics.mean(ratios) * 100.0), 2)


# ─── scores individuais (0–100) ───────────────────────────────────────────────

def _s_pf(pf: float) -> float:
    # PF=1.0→0, PF=2.0→100
    return round(_clamp((pf - 1.0) * 100.0), 2)


def _s_expectativa(exp: float) -> float:
    # exp=0→0, exp=20+→100
    return round(_clamp(exp * 5.0), 2)


def _s_payoff(payoff: float) -> float:
    # payoff=0→0, payoff=3+→100
    return round(_clamp(payoff * 33.33), 2)


def _s_drawdown(dd: float, resultado: float) -> float:
    if resultado <= 0:
        return 0.0
    ratio = dd / resultado
    # ratio=0→100, ratio=3+→0
    return round(_clamp((1.0 - min(ratio, 3.0) / 3.0) * 100.0), 2)


def _s_recovery(rf: float) -> float:
    # rf=0→0, rf=3+→100
    return round(_clamp(rf * 33.3), 2)


def _s_seq_perdas(max_seq: int) -> float:
    # seq=0→100, seq=20+→0
    return round(_clamp(100.0 - max_seq * 5.0), 2)


# ─── componentes de score ─────────────────────────────────────────────────────

def score_performance(pf: float, expectativa: float, payoff: float) -> Dict:
    sp = _s_pf(pf)
    se = _s_expectativa(expectativa)
    so = _s_payoff(payoff)
    total = round(0.45 * sp + 0.35 * se + 0.20 * so, 2)
    return {"score": total, "score_pf": sp, "score_expectativa": se, "score_payoff": so}


def score_risco(dd: float, resultado: float, rf: float, max_seq: int) -> Dict:
    sd = _s_drawdown(dd, resultado)
    sr = _s_recovery(rf)
    ss = _s_seq_perdas(max_seq)
    total = round(0.40 * sd + 0.35 * sr + 0.25 * ss, 2)
    return {"score": total, "score_drawdown": sd, "score_recovery": sr, "score_sequencia": ss}


def score_consistencia(pct_meses: float, pct_semanas: float,
                       dist_score: float, conc_score: float) -> Dict:
    sm = round(pct_meses * 100.0, 2)
    ss = round(pct_semanas * 100.0, 2)
    total = round(0.35 * sm + 0.25 * ss + 0.25 * dist_score + 0.15 * conc_score, 2)
    return {"score": total, "score_meses": sm, "score_semanas": ss,
            "score_distribuicao": dist_score, "score_concentracao": conc_score}


def score_robustez(bootstrap: float, monte_carlo: float,
                   oos: float, wf: float) -> Dict:
    total = round(0.30 * bootstrap + 0.30 * monte_carlo + 0.25 * oos + 0.15 * wf, 2)
    return {"score": total, "score_bootstrap": bootstrap,
            "score_monte_carlo": monte_carlo, "score_oos": oos, "score_wf": wf}


# ─── score bruto completo ─────────────────────────────────────────────────────

def calcular_score_bruto_completo(
    trades: List[float],
    statuses: List[str],
    trades_com_data: List[Dict],
) -> Dict:
    if not trades:
        return {"score_bruto": 0.0, "classificacao": "Sem dados", "componentes": {}, "metricas": {}, "curva_capital": [], "curva_drawdown": []}

    pf = calcular_profit_factor(trades)
    exp = calcular_expectativa(trades)
    payoff = calcular_payoff(trades)
    curva, dd = calcular_curva_e_drawdown(trades)
    curva_dd = calcular_curva_drawdown(trades)
    resultado = sum(trades)
    rf = round(resultado / dd, 4) if dd > 0 else 0.0
    max_seq = calcular_sequencia_max_perdas(statuses)
    total = len(trades)
    wins = sum(1 for s in statuses if s == "WIN")
    losses = total - wins
    win_rate = round(100.0 * wins / total, 2) if total > 0 else 0.0

    pct_meses = calcular_meses_positivos(trades_com_data)
    pct_semanas = calcular_semanas_positivas(trades_com_data)
    dist_score = calcular_distribuicao_lucro_score(trades)
    conc_score = calcular_concentracao_temporal_score(trades_com_data)

    bootstrap = calcular_bootstrap(trades)
    mc = calcular_monte_carlo(trades)
    oos = calcular_out_of_sample(trades)
    wf = calcular_walk_forward(trades)

    c_perf = score_performance(pf, exp, payoff)
    c_risco = score_risco(dd, resultado, rf, max_seq)
    c_consist = score_consistencia(pct_meses, pct_semanas, dist_score, conc_score)
    c_robust = score_robustez(bootstrap, mc, oos, wf)

    score_bruto = round(
        0.35 * c_perf["score"]
        + 0.25 * c_risco["score"]
        + 0.20 * c_consist["score"]
        + 0.20 * c_robust["score"],
        2,
    )

    classificacao = (
        "Excelente" if score_bruto >= 80 else
        "Bom" if score_bruto >= 60 else
        "Regular" if score_bruto >= 40 else
        "Fraco"
    )

    return {
        "score_bruto": score_bruto,
        "classificacao": classificacao,
        "componentes": {
            "performance": c_perf,
            "risco": c_risco,
            "consistencia": c_consist,
            "robustez": c_robust,
        },
        "metricas": {
            "profit_factor": round(pf, 2),
            "expectativa_pips": round(exp, 2),
            "payoff": round(payoff, 2),
            "drawdown_pips": round(dd, 2),
            "resultado_pips": round(resultado, 2),
            "recovery_factor": round(rf, 2),
            "max_seq_perdas": max_seq,
            "win_rate": win_rate,
            "wins": wins,
            "losses": losses,
            "total_trades": total,
        },
        "curva_capital": curva,
        "curva_drawdown": curva_dd,
    }


# ─── DHA scoring ──────────────────────────────────────────────────────────────

def calcular_score_dha(trades: List[float], statuses: List[str]) -> float:
    """Score DHA usando a mesma metodologia do Score Bruto."""
    if not trades:
        return 0.0
    pf = calcular_profit_factor(trades)
    exp = calcular_expectativa(trades)
    payoff = calcular_payoff(trades)
    _, dd = calcular_curva_e_drawdown(trades)
    resultado = sum(trades)
    rf = resultado / dd if dd > 0 else 0.0
    max_seq = calcular_sequencia_max_perdas(statuses)

    win_rate = sum(1 for s in statuses if s == "WIN") / len(statuses) if statuses else 0.5

    c_perf = score_performance(pf, exp, payoff)
    c_risco = score_risco(dd, resultado, rf, max_seq)
    # Para DHAs com amostra pequena: consistência via win rate, robustez via bootstrap leve
    c_consist = {"score": round(win_rate * 100.0, 2)}
    c_robust = {"score": calcular_bootstrap(trades, n_iter=100)}

    return round(
        0.35 * c_perf["score"]
        + 0.25 * c_risco["score"]
        + 0.20 * c_consist["score"]
        + 0.20 * c_robust["score"],
        2,
    )


def calcular_ic_dha(n_dha: int, o_dha: int, cv_dha: float,
                    ct_dha: float, cl_dha: float) -> float:
    """Índice de Confiabilidade do DHA (0-1)."""
    fa = min(1.0, n_dha / 100.0)       # Fator de Amostragem
    fo = min(1.0, o_dha / 20.0)        # Fator de Ocorrências
    fr = max(0.0, 1.0 - cv_dha)        # Fator de Regularidade
    fd = 1.0 - max(ct_dha, cl_dha)     # Fator de Distribuição Temporal
    return round(fa * fo * fr * fd, 6)