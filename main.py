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
        DATE_TRUNC('month', completed_at - INTERVAL '5 hours')::date AS mes,
        DATE_TRUNC('week', completed_at - INTERVAL '5 hours')::date AS semana
    FROM asana_fulfillment_task aft
    WHERE DATE_TRUNC('year', completed_at - INTERVAL '5 hours') >= DATE '2025-01-01'
),
-- 🔹 Menús por mes (solo New Menu)
resumen_mensual AS (
    SELECT mes, COUNT(*) AS menus_x_mes
    FROM base
    WHERE section_name = 'Done'
      AND type_menu = 'New Menu'
    GROUP BY mes
),
-- 🔹 Menús diarios (solo New Menu)
resumen_diario AS (
    SELECT fecha, COUNT(*) AS menus_x_dia
    FROM base
    WHERE section_name = 'Done'
      AND type_menu = 'New Menu'
    GROUP BY fecha
),
-- 🔹 Menús semanales (solo New Menu)
resumen_semanal AS (
    SELECT semana, COUNT(*) AS menus_x_week
    FROM base
    WHERE section_name = 'Done'
      AND type_menu = 'New Menu'
    GROUP BY semana
),
-- 🔹 Totales generales
conteos AS (
    SELECT
        COUNT(*) FILTER (WHERE type_menu = 'New Menu' AND section_name = 'Done') AS total_new_menu_done,
        COUNT(*) FILTER (WHERE type_menu = 'Update'   AND section_name = 'Done') AS total_update_done
    FROM base
),
-- 🔹 Persona top
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
-- 🔹 Promedio mensual
promedio_mes AS (
    SELECT ROUND(AVG(menus_x_mes),2) AS promedio_mes
    FROM resumen_mensual
),
-- 🔹 Mes más top (solo New Menu)
mes_top AS (
    SELECT
        TO_CHAR(mes, 'Mon YYYY') AS mes_mas_menus,
        menus_x_mes AS total_menus_mes_top
    FROM resumen_mensual
    ORDER BY menus_x_mes DESC
    LIMIT 1
),
-- 🔹 Día más top (solo New Menu)
dia_top AS (
    SELECT
        TO_CHAR(fecha, 'DD - Mon - YYYY') AS dia_mas_menus,
        menus_x_dia AS total_menus_dia_top
    FROM resumen_diario
    ORDER BY menus_x_dia DESC
    LIMIT 1
),
-- 🔹 Semana más top (solo New Menu)
week_top AS (
    SELECT
        TO_CHAR(semana, 'DD - Mon - YYYY') AS semana_mas_menus,
        menus_x_week AS total_menus_week_top
    FROM resumen_semanal
    ORDER BY menus_x_week DESC
    LIMIT 1
),
-- 🔹 Managers
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
-- 🔹 Total equipo Fulfillment activo
team_fulfillment AS (
    SELECT COUNT(*) AS total_team
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Fulfillment'
      AND status = 'Activo'
      AND role <> 'Manager'
),
-- 🔹 Personas que han salido (por tipo)
salidas_fulfillment AS (
    SELECT
        COUNT(*) FILTER (WHERE status ILIKE '%voluntario%') AS Voluntario,
        COUNT(*) FILTER (WHERE status ILIKE '%despido%') AS Despido
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Fulfillment'
      AND role <> 'Manager'
      AND status <> 'Activo'
),
-- 🔹 Periodo de datos dinámico
periodo_datos AS (
    SELECT
        TO_CHAR(MIN(mes), 'Mon YYYY') || ' - ' || TO_CHAR(MAX(mes), 'Mon YYYY') AS periodo
    FROM base
)
-- 🔹 Selección final
SELECT
 	pd.periodo 						 AS period,
    c.total_new_menu_done            AS total_new_menus,
    c.total_update_done              AS total_updated_menus,
    pm.promedio_mes                  AS monthly_average,
    m.mes_mas_menus                  AS top_month_name,
    m.total_menus_mes_top            AS top_month_total,
    w.semana_mas_menus               AS top_week_name,
    w.total_menus_week_top           AS top_week_total,
    d.dia_mas_menus                  AS top_day_name,
    d.total_menus_dia_top            AS top_day_total,
    p.assigne_name                   AS top_person_name,
    p.total_menus_persona_top        AS top_person_total,
    (s.Voluntario + s.Despido)       AS total_rotation,
    s.Voluntario                     AS voluntary,
    s.Despido                        AS dismissal,
    mgr.manager_name AS manager,
    tf.total_team
FROM conteos c
CROSS JOIN persona_top p
LEFT JOIN managers mgr
    ON mgr.team = 'Fulfillment'
CROSS JOIN promedio_mes pm
CROSS JOIN mes_top m
CROSS JOIN week_top w
CROSS JOIN dia_top d
CROSS JOIN salidas_fulfillment s
CROSS JOIN team_fulfillment tf
CROSS JOIN periodo_datos pd;
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
        DATE_TRUNC('month', completed_at - INTERVAL '5 hours')::date AS mes,
        DATE_TRUNC('week', completed_at - INTERVAL '5 hours')::date AS semana
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
-- 🔹 Total In Progress
total_in_progress AS (
    SELECT COUNT(DISTINCT gid) AS total_in_progress
    FROM asana_installations_task
    WHERE section_name IN (
        'Pending Shipment', 'Shipped (DEN)', 'Pending',
        'Arrived', 'First Contact', '15-30 Days',
        'Programada', 'Momias'
    )
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
-- 🔹 Resumen semanal
resumen_semanal AS (
    SELECT semana, COUNT(DISTINCT gid) AS instalaciones_x_week
    FROM base
    WHERE section_name = 'Installed' AND completed = true
    GROUP BY semana
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
-- 🔹 Mes con más instalaciones (bonito)
mes_top AS (
    SELECT 
        TO_CHAR(mes, 'Mon YYYY') AS mes_mas_instalaciones,
        instalaciones_x_mes
    FROM resumen_mensual
    ORDER BY instalaciones_x_mes DESC
    LIMIT 1
),
-- 🔹 Día con más instalaciones (bonito)
dia_top AS (
    SELECT 
        TO_CHAR(fecha, 'DD - Mon - YYYY') AS dia_mas_instalaciones,
        instalaciones_x_dia
    FROM resumen_diario_total
    ORDER BY instalaciones_x_dia DESC
    LIMIT 1
),
-- 🔹 Semana con más instalaciones (bonito)
week_top AS (
    SELECT 
        TO_CHAR(semana, 'DD - Mon - YYYY') AS semana_mas_instalaciones,
        instalaciones_x_week
    FROM resumen_semanal
    ORDER BY instalaciones_x_week DESC
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
-- 🔹 Totales del team Product Launch
team_info AS (
    SELECT
        COUNT(*) FILTER (WHERE status = 'Activo') AS total_activos,
        COUNT(*) FILTER (WHERE status <> 'Activo') AS total_no_activos,
        COUNT(*) FILTER (WHERE status ILIKE '%voluntario%') AS total_voluntarios,
        COUNT(*) FILTER (WHERE status ILIKE '%despido%') AS total_despidos
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Product Launch'
      AND role <> 'Manager'
),
-- 🔹 Periodo dinámico
periodo_datos AS (
    SELECT 
        TO_CHAR(MIN(mes), 'Mon YYYY') || ' - ' || TO_CHAR(MAX(mes), 'Mon YYYY') AS periodo
    FROM base
)
-- 🔹 Resultado final
SELECT
    pd.periodo                             AS period,
    ti.total_instaladas                    AS total_installed,
    tip.total_in_progress                  AS total_in_progress,
    pt.promedio_tabletas_mensual           AS monthly_average,
    m.mes_mas_instalaciones                AS top_month_name,
    m.instalaciones_x_mes                  AS top_month_total,
    w.semana_mas_instalaciones             AS top_week_name,
    w.instalaciones_x_week                 AS top_week_total,
    d.dia_mas_instalaciones                AS top_day_name,
    d.instalaciones_x_dia                  AS top_day_total,
    td.total_denegadas                     AS total_denied,
    mt.denied_reason                       AS common_denied_reason,
    mt.total_motivo                        AS total_reason_count,
    persona_overall.assigne_name           AS top_person_name,
    persona_overall.total_instalaciones    AS top_person_total,
    tii.total_no_activos                   AS total_rotation,
    tii.total_voluntarios                  AS voluntary,
    tii.total_despidos                     AS dismissal,
    mgr.manager_name                       AS manager_name,
    tii.total_activos                      AS total_team
FROM total_instaladas ti
CROSS JOIN total_in_progress tip
CROSS JOIN mes_top m
CROSS JOIN dia_top d
CROSS JOIN week_top w
CROSS JOIN persona_overall
CROSS JOIN total_denegadas td
CROSS JOIN motivo_top mt
LEFT JOIN managers mgr ON mgr.team = 'Google Integration'
CROSS JOIN team_info tii
CROSS JOIN promedio_tabletas pt
CROSS JOIN periodo_datos pd;
    """
    
    cur.execute(query)
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    result = dict(zip(columns, row))

    cur.close()
    conn.close()
    return result
