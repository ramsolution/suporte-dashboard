"""
extract_data.py — Extração de dados Chatwoot para análise de suporte
Sistema RAM | Squad: chatwoot-analytics
Gera data.js com dados diários para filtros client-side no dashboard
"""

import psycopg2
import json
import os
from datetime import datetime, date, timezone, timedelta

BRT = timezone(timedelta(hours=-3))  # America/Sao_Paulo (sem DST)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "34.151.226.30"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME", "chatwoot_production"),
    "user":     os.getenv("DB_USER", "qkl8FlrnaobApyze"),
    "password": os.getenv("DB_PASS", "Ou6i128ylVezLDuSJV0YHY9JH19mYYMd"),
    "connect_timeout": 30,
}

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Filtro para excluir conversas com label 'finalizar-silencioso' de todas as análises
EXCLUIR_LABEL = "finalizar-silencioso"

# Condição SQL reutilizável para excluir a label em tabelas com cached_label_list
# Usar com alias da tabela conversations: ex. _FILTRO_LABEL.format(alias='c')
_FILTRO_LABEL = (
    "NOT ('{label}' = ANY(string_to_array(COALESCE({{alias}}.cached_label_list, ''), ',')))"
    .format(label=EXCLUIR_LABEL)
)


def filtro_label(alias=""):
    """Retorna cláusula SQL para excluir conversas com EXCLUIR_LABEL.
    alias: prefixo de tabela (ex: 'c', 'co'). Se vazio, sem prefixo."""
    col = f"{alias}.cached_label_list" if alias else "cached_label_list"
    return f"NOT ('{EXCLUIR_LABEL}' = ANY(string_to_array(COALESCE({col}, ''), ',')))"


def connect():
    return psycopg2.connect(**DB_CONFIG)


def fetchall(cur, sql, label="query"):
    """Executa query e retorna rows; em erro retorna [] e imprime aviso."""
    try:
        cur.execute(sql)
        return cur.fetchall()
    except Exception as e:
        print(f"[WARN] Falha em '{label}': {e}")
        return []


def serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def extract_all():
    conn = connect()
    cur = conn.cursor()
    data = {}
    print("[OK] Conectado ao banco chatwoot_production")
    print(f"[INFO] Excluindo conversas com label '{EXCLUIR_LABEL}' de todas as análises")

    # ── 1. Dados diarios por agente (filtro principal) ──────────────────────
    print("> Extraindo dados diarios por agente...")
    rows = fetchall(cur, f"""
        SELECT TO_CHAR(c.created_at, 'YYYY-MM-DD') as dia,
               COALESCE(u.name, 'Nao atribuido') as agente,
               COUNT(*) as total,
               SUM(CASE WHEN c.status=1 THEN 1 ELSE 0 END) as resolvidas
        FROM conversations c
        LEFT JOIN users u ON c.assignee_id=u.id
        WHERE (c.assignee_id IS NULL OR c.assignee_id != 1)
          AND {filtro_label('c')}
        GROUP BY dia, agente ORDER BY dia
    """, "dados_diarios_agente")
    data["por_dia_agente"] = [
        {"d": r[0], "ag": r[1], "t": r[2], "res": r[3]} for r in rows
    ]

    # ── 2. Dados diarios CSAT ───────────────────────────────────────────────
    print("> Extraindo CSAT diario...")
    rows = fetchall(cur, f"""
        SELECT TO_CHAR(cs.created_at, 'YYYY-MM-DD') as dia,
               COALESCE(u.name, 'N/A') as agente,
               ROUND(AVG(cs.rating)::numeric,2) as media,
               COUNT(*) as total
        FROM csat_survey_responses cs
        LEFT JOIN users u ON cs.assigned_agent_id=u.id
        LEFT JOIN conversations co ON co.id = cs.conversation_id
        WHERE {filtro_label('co')}
        GROUP BY dia, agente ORDER BY dia
    """, "csat_diario")
    data["csat_por_dia"] = [
        {"d": r[0], "ag": r[1], "m": float(r[2]), "t": r[3]} for r in rows
    ]

    # Distribuicao de ratings (estatica)
    rows = fetchall(cur, f"""
        SELECT cs.rating, COUNT(*)
        FROM csat_survey_responses cs
        LEFT JOIN conversations co ON co.id = cs.conversation_id
        WHERE {filtro_label('co')}
        GROUP BY cs.rating ORDER BY cs.rating
    """, "csat_distribuicao")
    data["csat_dist"] = [{"r": r[0], "t": r[1]} for r in rows]

    # ── 3a. TMR (conversation_resolved) — via reporting_events ─────────────
    # TMR é confiável em reporting_events pois resolução sempre tem user_id humano.
    print("> Extraindo TMR diario (reporting_events)...")
    rows_tmr = fetchall(cur, f"""
        SELECT TO_CHAR(re.created_at, 'YYYY-MM-DD') as dia,
               COALESCE(u.name, 'N/A') as agente,
               re.name as evento,
               ROUND(AVG(re.value)::numeric,0) as avg_sec,
               COUNT(*) as total
        FROM reporting_events re
        LEFT JOIN users u ON re.user_id=u.id
        LEFT JOIN conversations co ON co.id = re.conversation_id
        WHERE re.name = 'conversation_resolved'
          AND (re.user_id IS NULL OR re.user_id != 1)
          AND {filtro_label('co')}
        GROUP BY dia, agente, re.name ORDER BY dia
    """, "tmr_diario")

    # ── 3b. TFR (first_response) — calculado direto da tabela messages ─────
    # reporting_events.first_response é one-shot: se o bot responde primeiro,
    # o evento já foi gravado com valor baixo e o agente humano não gera novo.
    # Solução: buscar a primeira mensagem outgoing com sender_type='User' por
    # conversa, medindo o tempo real de resposta humana desde a criação.
    print("> Extraindo TFR diario (primeira resposta humana via messages)...")
    rows_tfr = fetchall(cur, f"""
        SELECT TO_CHAR(c.created_at, 'YYYY-MM-DD') as dia,
               COALESCE(u.name, 'N/A') as agente,
               'first_response' as evento,
               ROUND(AVG(
                   EXTRACT(EPOCH FROM (fh.created_at - c.created_at))
               )::numeric,0) as avg_sec,
               COUNT(*) as total
        FROM conversations c
        JOIN LATERAL (
            SELECT m.created_at, m.sender_id
            FROM messages m
            INNER JOIN users u2 ON u2.id = m.sender_id AND u2.id != 1
            WHERE m.conversation_id = c.id
              AND m.message_type = 1
            ORDER BY m.created_at ASC
            LIMIT 1
        ) fh ON true
        LEFT JOIN users u ON fh.sender_id = u.id
        WHERE {filtro_label('c')}
        GROUP BY TO_CHAR(c.created_at, 'YYYY-MM-DD'), u.name
        ORDER BY dia
    """, "tfr_diario_humano")

    rows = rows_tmr + rows_tfr
    data["eventos_por_dia"] = [
        {"d": r[0], "ag": r[1], "ev": r[2], "s": float(r[3]), "t": r[4]}
        for r in rows
    ]

    # ── 4. Labels diarios ───────────────────────────────────────────────────
    print("> Extraindo labels diarios...")
    rows = fetchall(cur, f"""
        SELECT TO_CHAR(created_at, 'YYYY-MM-DD') as dia,
               TRIM(unnest(string_to_array(cached_label_list, ','))) as label,
               COUNT(*) as total
        FROM conversations
        WHERE cached_label_list IS NOT NULL AND cached_label_list != ''
          AND {filtro_label()}
        GROUP BY dia, label ORDER BY dia
    """, "labels_diarios")
    data["labels_por_dia"] = [{"d": r[0], "l": r[1], "t": r[2]} for r in rows]

    # ── 5. Hora do dia e dia da semana diarios ──────────────────────────────
    print("> Extraindo padroes horarios diarios...")
    rows = fetchall(cur, f"""
        SELECT TO_CHAR(created_at, 'YYYY-MM-DD') as dia,
               EXTRACT(HOUR FROM created_at)::int as hora,
               EXTRACT(DOW FROM created_at)::int as dow,
               COUNT(*) as total
        FROM conversations
        WHERE {filtro_label()}
        GROUP BY dia, hora, dow ORDER BY dia
    """, "padroes_horarios")
    data["hora_dow_por_dia"] = [
        {"d": r[0], "h": r[1], "dow": r[2], "t": r[3]} for r in rows
    ]

    # ── 6. Status geral (estatico) ──────────────────────────────────────────
    rows = fetchall(cur, f"""
        SELECT status, COUNT(*) FROM conversations
        WHERE {filtro_label()}
        GROUP BY status
    """, "status_geral")
    sm = {0: "open", 1: "resolved", 2: "pending", 3: "snoozed"}
    data["status_total"] = {sm.get(r[0], str(r[0])): r[1] for r in rows}

    # ── 7. Dados estaticos (equipes, inboxes, agentes) ──────────────────────
    rows = fetchall(cur, "SELECT id, name FROM teams", "equipes")
    data["equipes"] = [{"id": r[0], "nome": r[1]} for r in rows]

    rows = fetchall(cur, "SELECT id, name, channel_type FROM inboxes", "inboxes")
    data["inboxes"] = [{"id": r[0], "nome": r[1], "tipo": r[2]} for r in rows]

    rows = fetchall(cur, "SELECT id, name, email FROM users ORDER BY id", "agentes")
    data["agentes"] = [{"id": r[0], "nome": r[1], "email": r[2]} for r in rows]

    rows = fetchall(cur, f"""
        SELECT COALESCE(t.name,'Sem equipe') as equipe, COUNT(c.id) as total
        FROM conversations c
        LEFT JOIN teams t ON c.team_id=t.id
        WHERE {filtro_label('c')}
        GROUP BY equipe ORDER BY total DESC
    """, "conversas_por_equipe")
    data["conversas_por_equipe"] = [{"eq": r[0], "t": r[1]} for r in rows]

    # Mensagens por tipo (estatico)
    rows = fetchall(cur,
        "SELECT message_type, COUNT(*) FROM messages GROUP BY message_type", "msg_tipos")
    mt = {0:"incoming", 1:"outgoing", 2:"activity", 3:"template"}
    data["msg_tipos"] = {mt.get(r[0], str(r[0])): r[1] for r in rows}

    # ── 8. Auditoria TFR: conversas com bot como primeiro respondente ────────
    # Conta conversas onde o bot (sender_type != 'User') respondeu antes do humano.
    print("> Auditando conversas com bot respondendo primeiro...")
    rows = fetchall(cur, f"""
        SELECT COUNT(DISTINCT c.id) as total
        FROM conversations c
        JOIN LATERAL (
            SELECT m.sender_type
            FROM messages m
            WHERE m.conversation_id = c.id
              AND m.message_type = 1
            ORDER BY m.created_at ASC
            LIMIT 1
        ) first_msg ON true
        WHERE first_msg.sender_type != 'User'
          AND {filtro_label('c')}
    """, "bot_first_response_audit")
    data["bot_first_response_ignorados"] = rows[0][0] if rows else 0
    if data["bot_first_response_ignorados"] > 0:
        print(f"   [INFO] {data['bot_first_response_ignorados']} conversas com bot como primeiro respondente (TFR medido a partir do humano)")

    # ── 9. Configuracao SLA (exportada para o dashboard) ────────────────────
    data["sla_config"] = {
        "tmr_h": 24,
        "tmr_warn_h": 18,
        "tmr_crit_h": 36,
        "tfr_min": 30,
        "tfr_warn_min": 15,
        "csat_excellent": 4.8,
        "csat_good": 4.5,
        "res_rate_ok": 95,
        "survey_rate_ok": 15,
    }

    # ── Metadata ─────────────────────────────────────────────────────────────
    data["extraido_em"] = datetime.now(BRT).isoformat()
    data["total_mensagens"] = sum(data["msg_tipos"].values())

    conn.close()
    return data


def main():
    print("=" * 60)
    print("  Extracao de Dados - Suporte Sistema RAM / Chatwoot")
    print("=" * 60)

    data = extract_all()

    # Salva JSON
    json_path = os.path.join(OUTPUT_DIR, "dados_suporte.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=serialize)

    # Salva data.js (para dashboard GitHub Pages)
    js_path = os.path.join(OUTPUT_DIR, "data.js")
    with open(js_path, "w", encoding="utf-8") as f:
        f.write("/* Auto-gerado por extract_data.py — Sistema RAM */\n")
        f.write("const SUPORTE_DATA = ")
        json.dump(data, f, ensure_ascii=False, default=serialize)
        f.write(";\n")

    total = sum(r["t"] for r in data["por_dia_agente"])
    csat_vals = [r["m"] * r["t"] for r in data["csat_por_dia"]]
    csat_counts = [r["t"] for r in data["csat_por_dia"]]
    csat_media = round(sum(csat_vals) / sum(csat_counts), 2) if csat_counts else 0

    print(f"\n[DONE] JSON salvo: {json_path}")
    print(f"[DONE] data.js salvo: {js_path}")
    print(f"   Conversas: {total}")
    print(f"   Mensagens: {data['total_mensagens']}")
    print(f"   CSAT medio: {csat_media}/5")
    bot_ignored = data.get("bot_first_response_ignorados", 0)
    if bot_ignored:
        print(f"   TFR bot ignorados: {bot_ignored} eventos (first_response sem user_id)")
    print(f"   Label excluída: '{EXCLUIR_LABEL}'")
    print("\n[INFO] Execute index.html no navegador para visualizar.")


if __name__ == "__main__":
    main()
