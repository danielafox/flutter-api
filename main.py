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

# 🚀 Endpoint: resumen de menús
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
        SELECT assigne_name, COUNT(*) AS total_menus_persona_top
        FROM base
        WHERE section_name = 'Done'
        AND type_menu IN ('New Menu','Update')
        AND assigne_name IS NOT NULL
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
        ),
        managers AS (
        SELECT
        sir.name AS manager_name,
        sir.team,
        sir.department,
        sir.role
        FROM sheets_ingresos_y_retiros sir
        WHERE department = 'Operations'
        AND team IN ('Fulfillment','Product Launch','Google Integration')
        AND role LIKE('%Manager%')
        ),
        team_fulfillment AS (
        SELECT COUNT(*) AS total_team
        FROM sheets_ingresos_y_retiros sir
        WHERE department = 'Operations'
        AND team = 'Fulfillment'
        )
        SELECT
        c.total_new_menu_done,
        c.total_update_done,
        p.assigne_name AS persona_top,
        p.total_menus_persona_top AS total_persona_top,
        pm.promedio_mes,
        m.mes_mas_menus,
        m.total_menus_mes_top,
        d.dia_mas_menus,
        d.total_menus_dia_top,
        mgr.manager_name AS manager,
        tf.total_team
        FROM conteos c
        CROSS JOIN persona_top p
        LEFT JOIN managers mgr
        ON mgr.team = 'Fulfillment'
        CROSS JOIN promedio_mes pm
        CROSS JOIN mes_top m
        CROSS JOIN dia_top d
        CROSS JOIN team_fulfillment tf;
    """

    cur.execute(query)
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    result = dict(zip(columns, row))

    cur.close()
    conn.close()
    return result

# 🚀 Nuevo endpoint: resumen de instalaciones
@app.get("/resumen_installations")
def obtener_resumen_installations():
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
    WITH base AS (
    SELECT
        gid,
        TRIM(assigne_name)    AS assigne_name,
        TRIM(country)         AS country,
        denied_reason,
        TRIM(section_name)    AS section_name,
        completed,
        (completed_at - INTERVAL '5 hours')::date AS fecha,
        DATE_TRUNC('month', completed_at - INTERVAL '5 hours')::date AS mes
    FROM asana_installations_task ait
    WHERE completed_at IS NOT NULL
      AND DATE_TRUNC('year', completed_at - INTERVAL '5 hours') >= DATE '2025-01-01'
),
-- 🔹 Base separada para Denied (usa created_at)
base_denied AS (
    SELECT
        gid,
        TRIM(assigne_name) AS assigne_name,
        TRIM(country)      AS country,
        denied_reason,
        TRIM(section_name) AS section_name,
        (created_at - INTERVAL '5 hours')::date AS fecha,
        DATE_TRUNC('month', created_at - INTERVAL '5 hours')::date AS mes
    FROM asana_installations_task ait
    WHERE section_name = 'Denied'
      AND DATE_TRUNC('year', created_at - INTERVAL '5 hours') >= DATE '2025-01-01'
),
-- 🔹 Total instaladas
total_instaladas AS (
    SELECT COUNT(DISTINCT gid) AS total_instaladas
    FROM base
    WHERE section_name = 'Installed'
      AND completed = true
),
-- 🔹 Resumen mensual
resumen_mensual AS (
    SELECT mes, COUNT(DISTINCT gid) AS instalaciones_x_mes
    FROM base
    WHERE section_name = 'Installed' AND completed = true
    GROUP BY mes
),
-- 🔹 Resumen diario
resumen_diario_total AS (
    SELECT fecha, COUNT(DISTINCT gid) AS instalaciones_x_dia
    FROM base
    WHERE section_name = 'Installed' AND completed = true
    GROUP BY fecha
),
-- 🔹 Promedio mensual de instalaciones
promedio_tabletas AS (
    SELECT 
        ROUND(
            COUNT(DISTINCT gid)::numeric / COUNT(DISTINCT mes),
            2
        ) AS promedio_tabletas_mensual
    FROM base
    WHERE section_name = 'Installed' AND completed = true
),
-- 🔹 Mes con más instalaciones
mes_top AS (
    SELECT mes, instalaciones_x_mes
    FROM resumen_mensual
    ORDER BY instalaciones_x_mes DESC
    LIMIT 1
),
-- 🔹 Día con más instalaciones
dia_top AS (
    SELECT fecha, instalaciones_x_dia
    FROM resumen_diario_total
    ORDER BY instalaciones_x_dia DESC
    LIMIT 1
),
-- 🔹 Persona con más instalaciones
persona_overall AS (
    SELECT assigne_name, COUNT(DISTINCT gid) AS total_instalaciones
    FROM base
    WHERE section_name = 'Installed' AND completed = true
      AND assigne_name IS NOT NULL
    GROUP BY assigne_name
    ORDER BY total_instalaciones DESC
    LIMIT 1
),
-- 🔹 Total denegadas
total_denegadas AS (
    SELECT COUNT(DISTINCT gid) AS total_denegadas
    FROM base_denied
),
-- 🔹 Motivo más común en denegadas
motivo_top AS (
    SELECT denied_reason, COUNT(DISTINCT gid) AS total_motivo
    FROM base_denied
    WHERE denied_reason IS NOT NULL
    GROUP BY denied_reason
    ORDER BY total_motivo DESC
    LIMIT 1
),
-- 🔹 Managers de Google Integration
managers AS (
    SELECT
        sir.name AS manager_name,
        sir.team,
        sir.department,
        sir.role
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Google Integration'
      AND role LIKE('%Manager%')
),
-- 🔹 Total del team Product Launch
total_team AS (
    SELECT COUNT(*) AS total_team
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Product Launch'
)
SELECT
    ti.total_instaladas                 AS total_installed,
    TO_CHAR(mes_top.mes,'YYYY-MM')      AS mes_top,
    mes_top.instalaciones_x_mes         AS total_mes_top,
    dia_top.fecha                       AS day_top,
    dia_top.instalaciones_x_dia         AS total_day_top,
    persona_overall.assigne_name        AS persona_top,
    persona_overall.total_instalaciones AS total_persona_top,
    td.total_denegadas                  AS total_denied,
    mt.denied_reason                    AS common_reason,
    mt.total_motivo                     AS total_reason,
    mgr.manager_name                    AS manager,
    tt.total_team                       AS total_team,
    pt.promedio_tabletas_mensual        AS avg_mes
FROM total_instaladas ti
CROSS JOIN mes_top
CROSS JOIN dia_top
CROSS JOIN persona_overall
CROSS JOIN total_denegadas td
CROSS JOIN motivo_top mt
LEFT JOIN managers mgr ON mgr.team = 'Google Integration'
CROSS JOIN total_team tt
CROSS JOIN promedio_tabletas pt;
    """
    
    cur.execute(query)
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    result = dict(zip(columns, row))

    cur.close()
    conn.close()
    return result
