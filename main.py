from fastapi import FastAPI
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
print("DATABASE_URL =", DATABASE_URL)


def get_connection():
    return psycopg2.connect(DATABASE_URL)


@app.get("/")
def home():
    return {"status": "API rodando"}


@app.get("/test-db")
def test_db():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT 1;")

        cur.close()
        conn.close()

        return {"status": "conectado ao banco"}

    except Exception as e:
        return {"status": "erro", "mensagem": str(e)}


@app.post("/operacoes")
def receber_operacao(data: dict):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO operacoes (
                id_operacao,
                ativo,
                tipo_ordem,
                preco_entrada,
                resultado_financeiro
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_operacao) DO NOTHING
        """, (
            data.get("IDOperacao"),
            data.get("Ativo"),
            data.get("TipoOrdem"),
            data.get("PrecoEntrada"),
            data.get("ResultadoFinanceiro")
        ))

        conn.commit()

        cur.close()
        conn.close()

        return {
            "status": "sucesso",
            "id_operacao": data.get("IDOperacao")
        }

    except Exception as e:
        return {
            "status": "erro",
            "mensagem": str(e)
        }
