# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2

app = FastAPI()

# 🚨 Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todos los orígenes (puedes restringirlo a ["http://localhost:xxxx"])
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_connection():
    return psycopg2.connect(
        host="162.243.164.24",
        dbname="foxordering_data",
        user="databi",
        password="yqtj0rcGg*l",
        port=5432
    )

@app.get("/")
def home():
    return {"mensaje": "API con PostgreSQL funcionando 🚀"}

@app.get("/clientes")
def obtener_clientes():
    conn = get_connection()
    cur = conn.cursor()
    query = """
        SELECT *
        FROM asana_churned_task
        LIMIT 10;
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    result = [dict(zip(columns, row)) for row in rows]
    cur.close()
    conn.close()
    return result
