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
        TO_CHAR(MIN(mes), 'Mon YYYY') || ' - ' ||
        TO_CHAR((CURRENT_TIMESTAMP - INTERVAL '5 hours')::date, 'DD Mon YYYY') AS periodo
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
total_instaladas AS (
    SELECT COUNT(DISTINCT gid) AS total_instaladas
    FROM base
    WHERE section_name = 'Installed' AND completed = true
),
total_in_progress AS (
    SELECT COUNT(DISTINCT gid) AS total_in_progress
    FROM asana_installations_task
    WHERE section_name IN (
        'Pending Shipment', 'Shipped (DEN)', 'Pending',
        'Arrived', 'First Contact', '15-30 Days',
        'Programada', 'Momias'
    )
),
resumen_mensual AS (
    SELECT mes, COUNT(DISTINCT gid) AS instalaciones_x_mes
    FROM base
    WHERE section_name = 'Installed' AND completed = true
    GROUP BY mes
),
resumen_diario_total AS (
    SELECT fecha, COUNT(DISTINCT gid) AS instalaciones_x_dia
    FROM base
    WHERE section_name = 'Installed' AND completed = true
    GROUP BY fecha
),
resumen_semanal AS (
    SELECT semana, COUNT(DISTINCT gid) AS instalaciones_x_week
    FROM base
    WHERE section_name = 'Installed' AND completed = true
    GROUP BY semana
),
promedio_tabletas AS (
    SELECT 
        ROUND(
            COUNT(DISTINCT gid)::numeric / COUNT(DISTINCT mes),
            2
        ) AS promedio_tabletas_mensual
    FROM base
    WHERE section_name = 'Installed' AND completed = true
),
mes_top AS (
    SELECT 
        TO_CHAR(mes, 'Mon YYYY') AS mes_mas_instalaciones,
        instalaciones_x_mes
    FROM resumen_mensual
    ORDER BY instalaciones_x_mes DESC
    LIMIT 1
),
dia_top AS (
    SELECT 
        TO_CHAR(fecha, 'DD - Mon - YYYY') AS dia_mas_instalaciones,
        instalaciones_x_dia
    FROM resumen_diario_total
    ORDER BY instalaciones_x_dia DESC
    LIMIT 1
),
week_top AS (
    SELECT 
        TO_CHAR(semana, 'DD - Mon - YYYY') AS semana_mas_instalaciones,
        instalaciones_x_week
    FROM resumen_semanal
    ORDER BY instalaciones_x_week DESC
    LIMIT 1
),
persona_overall AS (
    SELECT assigne_name, COUNT(DISTINCT gid) AS total_instalaciones
    FROM base
    WHERE section_name = 'Installed' AND completed = true
      AND assigne_name IS NOT NULL
    GROUP BY assigne_name
    ORDER BY total_instalaciones DESC
    LIMIT 1
),
total_denegadas AS (
    SELECT COUNT(DISTINCT gid) AS total_denegadas
    FROM base_denied
),
motivo_top AS (
    SELECT denied_reason, COUNT(DISTINCT gid) AS total_motivo
    FROM base_denied
    WHERE denied_reason IS NOT NULL
    GROUP BY denied_reason
    ORDER BY total_motivo DESC
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
      AND team = 'Google Integration'
      AND role LIKE('%Manager%')
),
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
periodo_datos AS (
    SELECT
        TO_CHAR(MIN(mes), 'Mon YYYY') || ' - ' ||
        TO_CHAR((CURRENT_TIMESTAMP - INTERVAL '5 hours')::date, 'DD Mon YYYY') AS periodo
    FROM base
),
-- 🔹 País con más instalaciones
country_top AS (
    SELECT 
        country AS top_country,
        COUNT(DISTINCT gid) AS total_tablets_country
    FROM base
    WHERE section_name = 'Installed' AND completed = true
      AND country IS NOT NULL
    GROUP BY country
    ORDER BY total_tablets_country DESC
    LIMIT 1
)
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
    ct.top_country                         AS top_country,
    ct.total_tablets_country               AS top_country_total,
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
CROSS JOIN country_top ct
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

# 🚀 Nuevo endpoint: resumen QA
@app.get("/resumen_qa")
def obtener_resumen_qa():
    conn = get_connection()
    cur = conn.cursor()

    query = """
WITH base AS (
    SELECT
        gid,
        TRIM(assigne_name)        AS assigne_name,
        TRIM(section_name)        AS section_name,
        completed,
        TRIM(reason_pause)        AS reason_pause,
        TRIM(reason_lost)         AS reason_lost,
        (COALESCE(created_at, completed_at) - INTERVAL '5 hours')::date AS fecha_ref,
        DATE_TRUNC('month', COALESCE(created_at, completed_at) - INTERVAL '5 hours')::date AS mes,
        DATE_TRUNC('week',  COALESCE(created_at, completed_at) - INTERVAL '5 hours')::date AS semana
    FROM asana_qa_task aqt
    WHERE DATE_TRUNC('year', COALESCE(created_at, completed_at) - INTERVAL '5 hours') = DATE '2025-01-01'
),
--Totales generales
totales AS (
    SELECT
        COUNT(DISTINCT CASE WHEN section_name = 'Approved' THEN gid END) AS total_approved,
        COUNT(DISTINCT CASE WHEN section_name = 'In Progress' THEN gid END) AS total_in_progress,
        COUNT(DISTINCT CASE WHEN section_name = 'Paused' THEN gid END) AS total_paused,
        COUNT(DISTINCT CASE WHEN section_name = 'Rejected' THEN gid END) AS total_rejected
    FROM base
),
--Persona con más tareas
top_persona AS (
    SELECT
        assigne_name,
        COUNT(DISTINCT gid) AS total_tareas_persona
    FROM base
    WHERE assigne_name IS NOT NULL AND TRIM(assigne_name) <> '' AND section_name = 'Approved'
    GROUP BY assigne_name
    ORDER BY total_tareas_persona DESC
    LIMIT 1
),
--Razón más común de pausa
razon_pausa AS (
    SELECT
        reason_pause,
        COUNT(DISTINCT gid) AS total_por_razon
    FROM base
    WHERE section_name = 'Paused'
      AND reason_pause IS NOT NULL
      AND TRIM(reason_pause) <> ''
    GROUP BY reason_pause
    ORDER BY total_por_razon DESC
    LIMIT 1
),
--Razón más común de lost
razon_lost AS (
    SELECT
        reason_lost,
        COUNT(DISTINCT gid) AS total_reason_lost
    FROM base
    WHERE section_name = 'Rejected'
      AND reason_lost IS NOT NULL
      AND TRIM(reason_lost) <> ''
    GROUP BY reason_lost
    ORDER BY total_reason_lost DESC
    LIMIT 1
),
--Mes con más tareas Approved
mes_top AS (
    SELECT
        TO_CHAR(mes, 'Mon YYYY') AS mes_mas_approved,
        COUNT(DISTINCT gid) AS total_mes_top
    FROM base
    WHERE section_name = 'Approved'
    GROUP BY mes
    ORDER BY total_mes_top DESC
    LIMIT 1
),
--Semana con más tareas Approved
semana_top AS (
    SELECT
        TO_CHAR(semana, 'DD Mon YYYY') AS semana_mas_approved,
        COUNT(DISTINCT gid) AS total_semana_top
    FROM base
    WHERE section_name = 'Approved'
    GROUP BY semana
    ORDER BY total_semana_top DESC
    LIMIT 1
),
--Día con más tareas Approved
dia_top AS (
    SELECT
        TO_CHAR(fecha_ref, 'DD Mon YYYY') AS dia_mas_approved,
        COUNT(DISTINCT gid) AS total_dia_top
    FROM base
    WHERE section_name = 'Approved'
    GROUP BY fecha_ref
    ORDER BY total_dia_top DESC
    LIMIT 1
),
--Periodo de datos dinámico
periodo_datos AS (
    SELECT
        TO_CHAR(MIN(mes), 'Mon YYYY') || ' - ' ||
        TO_CHAR((CURRENT_TIMESTAMP - INTERVAL '5 hours')::date, 'DD Mon YYYY') AS period
    FROM base
),
--Manager info
managers AS (
    SELECT
        sir.name AS manager,
        sir.team,
        sir.department,
        sir.role
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Quality Assurance'
      AND role LIKE('%Manager%')
),
--Total equipo QA activo
team_qa AS (
    SELECT COUNT(*) AS total_team
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Quality Assurance'
      AND status = 'Activo'
      AND role <> 'Manager'
),
--Rotación QA
rotation AS (
    SELECT
        COUNT(*) FILTER (WHERE status ILIKE '%voluntario%') AS voluntario,
        COUNT(*) FILTER (WHERE status ILIKE '%despido%') AS despido
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Quality Assurance'
      AND role <> 'Manager'
      AND status <> 'Activo'
)
SELECT
    per.period,
    t.*,
    m.mes_mas_approved       AS top_month_name,
    m.total_mes_top          AS top_month_total,
    s.semana_mas_approved    AS top_week_name,
    s.total_semana_top       AS top_week_total,
    d.dia_mas_approved       AS top_day_name,
    d.total_dia_top          AS top_day_total,
    p.assigne_name           AS top_person_name,
    p.total_tareas_persona   AS top_person_total,
    r.reason_pause           AS common_paused_reason,
    r.total_por_razon        AS total_reason_paused,
    rl.reason_lost           AS common_rejected_reason,
    rl.total_reason_lost     AS total_reason_rejected,
    (rot.voluntario + rot.despido) AS total_rotation,
    rot.voluntario           AS voluntary,
    rot.despido              AS dismissal,
    man.manager,
    tq.total_team            AS total_team
FROM totales t
LEFT JOIN top_persona p     ON true
LEFT JOIN razon_pausa r     ON true
LEFT JOIN razon_lost rl     ON true
LEFT JOIN mes_top m         ON true
LEFT JOIN semana_top s      ON true
LEFT JOIN dia_top d         ON true
LEFT JOIN periodo_datos per ON true
LEFT JOIN managers man      ON true
LEFT JOIN team_qa tq        ON true
LEFT JOIN rotation rot      ON true;
    """

    cur.execute(query)
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    result = dict(zip(columns, row))

    cur.close()
    conn.close()
    return result

# 🚀 Nuevo endpoint: resumen Deal Creation
@app.get("/resumen_dealcreation")
def obtener_resumen_qa():
    conn = get_connection()
    cur = conn.cursor()

    query = """
WITH 
-- 🔹 Base para tareas DONE (usa completed_at)
base_done AS (
    SELECT
        gid,
        TRIM(assigne_name) AS assigne_name,
        TRIM(section_name) AS section_name,
        completed,
        (completed_at - INTERVAL '5 hours')::date AS fecha_ref,
        DATE_TRUNC('month', completed_at - INTERVAL '5 hours')::date AS mes,
        DATE_TRUNC('week',  completed_at - INTERVAL '5 hours')::date AS semana
    FROM asana_dealcreation_task adc
    WHERE section_name ='Done'
      AND completed_at IS NOT NULL
      AND DATE_TRUNC('year', completed_at - INTERVAL '5 hours') = DATE '2025-01-01'
),
-- 🔹 Base para tareas en progreso o no finalizadas (usa created_at)
base_in_progress AS (
    SELECT
        gid,
        TRIM(assigne_name) AS assigne_name,
        TRIM(section_name) AS section_name,
        completed,
        (created_at - INTERVAL '5 hours')::date AS fecha_ref,
        DATE_TRUNC('month', created_at - INTERVAL '5 hours')::date AS mes,
        DATE_TRUNC('week',  created_at - INTERVAL '5 hours')::date AS semana
    FROM asana_dealcreation_task adc
    WHERE section_name in ('Unassigned','Loading Page','In Progress','Paused')
      AND created_at IS NOT NULL
      AND DATE_TRUNC('year', created_at - INTERVAL '5 hours') = DATE '2025-01-01'
),
-- 🔹 Unión final de bases
base AS (
    SELECT * FROM base_done
    UNION ALL
    SELECT * FROM base_in_progress
),
-- 🔹 Totales generales
totales AS (
    SELECT
        COUNT(DISTINCT CASE WHEN section_name = 'Done' THEN gid END) AS total_done,
        COUNT(DISTINCT CASE WHEN section_name IN ('Unassigned','Loading Page','In Progress') THEN gid END) AS total_in_progress,
        COUNT(DISTINCT CASE WHEN section_name = 'Paused' THEN gid END) AS total_paused
    FROM base
),
-- 🔹 Persona con más tareas Done
top_persona AS (
    SELECT
        assigne_name,
        COUNT(DISTINCT gid) AS total_tareas_persona
    FROM base
    WHERE assigne_name IS NOT NULL AND TRIM(assigne_name) <> '' 
      AND section_name = 'Done'
    GROUP BY assigne_name
    ORDER BY total_tareas_persona DESC
    LIMIT 1
),
-- 🔹 Mes con más tareas Done
mes_top AS (
    SELECT
        TO_CHAR(mes, 'Mon YYYY') AS mes_mas_approved,
        COUNT(DISTINCT gid) AS total_mes_top
    FROM base
    WHERE section_name = 'Done'
    GROUP BY mes
    ORDER BY total_mes_top DESC
    LIMIT 1 
),
-- 🔹 Semana con más tareas Done
semana_top AS (
    SELECT
        TO_CHAR(semana, 'DD Mon YYYY') AS semana_mas_approved,
        COUNT(DISTINCT gid) AS total_semana_top
    FROM base
    WHERE section_name = 'Done'
    GROUP BY semana
    ORDER BY total_semana_top DESC
    LIMIT 1
),
-- 🔹 Día con más tareas Done
dia_top AS (
    SELECT
        TO_CHAR(fecha_ref, 'DD Mon YYYY') AS dia_mas_approved,
        COUNT(DISTINCT gid) AS total_dia_top
    FROM base
    WHERE section_name = 'Done'
    GROUP BY fecha_ref
    ORDER BY total_dia_top DESC
    LIMIT 1
),
-- 🔹 Periodo de datos dinámico
periodo_datos AS (
    SELECT
        TO_CHAR(MIN(mes), 'Mon YYYY') || ' - ' ||
        TO_CHAR((CURRENT_TIMESTAMP - INTERVAL '5 hours')::date, 'DD Mon YYYY') AS period
    FROM base
),
-- 🔹 Managers (referencia externa)
managers AS (
    SELECT
        sir.name AS manager,
        sir.team,
        sir.department,
        sir.role
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Business Operation'
      AND role LIKE('%Manager%')
),
-- 🔹 Total equipo activo
team_qa AS (
    SELECT COUNT(*) AS total_team
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Business Operation'
      AND status = 'Activo'
      AND role = 'System Specialist'
),
-- 🔹 Rotación
rotation AS (
    SELECT
        COUNT(*) FILTER (WHERE status ILIKE '%voluntario%') AS voluntario,
        COUNT(*) FILTER (WHERE status ILIKE '%despido%') AS despido
    FROM sheets_ingresos_y_retiros sir
    WHERE department = 'Operations'
      AND team = 'Business Operation'
      AND role = 'System Specialist'
      AND status <> 'Activo'
)
-- 🔹 Resultado final
SELECT
    per.period,
    t.*,
    m.mes_mas_approved AS top_month_name,
    m.total_mes_top AS top_month_total,
    s.semana_mas_approved AS top_week_name,
    s.total_semana_top AS top_week_total,
    d.dia_mas_approved AS top_day_name,
    d.total_dia_top AS top_day_total,
    p.assigne_name AS top_person_name,
    p.total_tareas_persona AS top_person_total,
    (rot.voluntario + rot.despido) AS total_rotation,
    rot.voluntario AS voluntary,
    rot.despido AS dismissal,
    man.manager,
    tq.total_team AS total_team
FROM totales t
LEFT JOIN top_persona p ON true
LEFT JOIN mes_top m ON true
LEFT JOIN semana_top s ON true
LEFT JOIN dia_top d ON true
LEFT JOIN periodo_datos per ON true
LEFT JOIN managers man ON true
LEFT JOIN team_qa tq ON true
LEFT JOIN rotation rot ON true;
    """

    cur.execute(query)
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    result = dict(zip(columns, row))

    cur.close()
    conn.close()
    return result
