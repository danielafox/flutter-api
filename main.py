# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2

app = FastAPI()

# 🚨 Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Puedes restringirlo a ["http://localhost:3000"]
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

# 🚀 Nuevo endpoint: resumen de menús
@app.get("/resumen_menus")
def obtener_resumen_menus():
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
    WITH base AS (
        SELECT
            gid,
            assigne_name,
            type_menu,
            section_name,
            (completed_at - INTERVAL '5 hours')::date AS fecha,
            DATE_TRUNC('month', completed_at - INTERVAL '5 hours')::date AS mes
        FROM asana_fulfillment_task aft
        WHERE DATE_TRUNC('year', completed_at - INTERVAL '5 hours') >= DATE '2025-01-01'
    ),
    resumen_mensual AS (
        SELECT mes, COUNT(*) AS menus_x_mes
        FROM base
        WHERE section_name = 'Done'
          AND type_menu IN ('New Menu','Update')
        GROUP BY mes
    ),
    resumen_diario AS (
        SELECT fecha, COUNT(*) AS menus_x_dia
        FROM base
        WHERE section_name = 'Done'
          AND type_menu IN ('New Menu','Update')
        GROUP BY fecha
    ),
    conteos AS (
        SELECT
            COUNT(*) FILTER (WHERE type_menu = 'New Menu' AND section_name = 'Done') AS total_new_menu_done,
            COUNT(*) FILTER (WHERE type_menu = 'Update'   AND section_name = 'Done') AS total_update_done
        FROM base
    ),
    persona_top AS (
        SELECT assigne_name
        FROM base
        WHERE section_name = 'Done'
          AND type_menu IN ('New Menu','Update')
        GROUP BY assigne_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ),
    promedio_mes AS (
        SELECT ROUND(AVG(menus_x_mes),2) AS promedio_mes
        FROM resumen_mensual
    ),
    mes_top AS (
        SELECT 
            TO_CHAR(mes,'YYYY-MM') AS mes_mas_menus,
            menus_x_mes AS total_menus_mes_top
        FROM resumen_mensual
        ORDER BY menus_x_mes DESC
        LIMIT 1
    ),
    dia_top AS (
        SELECT 
            fecha AS dia_mas_menus,
            menus_x_dia AS total_menus_dia_top
        FROM resumen_diario
        ORDER BY menus_x_dia DESC
        LIMIT 1
    )
    SELECT
        c.total_new_menu_done,
        c.total_update_done,
        p.assigne_name AS persona_top,
        pm.promedio_mes,
        m.mes_mas_menus,
        m.total_menus_mes_top,
        d.dia_mas_menus,
        d.total_menus_dia_top
    FROM conteos c
    CROSS JOIN persona_top p
    CROSS JOIN promedio_mes pm
    CROSS JOIN mes_top m
    CROSS JOIN dia_top d;
    """

    cur.execute(query)
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    result = dict(zip(columns, row))

    cur.close()
    conn.close()
    return result
