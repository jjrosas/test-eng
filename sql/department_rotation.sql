WITH 
empleados_salidos AS (
    SELECT 
        de.dept_no,
        COUNT(DISTINCT de.emp_no) AS cantidad_salidos
    FROM 
        dept_emp de
    WHERE 
        de.to_date < '9999-01-01'
    GROUP BY 
        de.dept_no
),
empleados_totales AS (
    SELECT 
        de.dept_no,
        COUNT(DISTINCT de.emp_no) AS cantidad_total
    FROM 
        dept_emp de
    GROUP BY 
        de.dept_no
)
SELECT 
    d.dept_no,
    d.dept_name AS nombre_departamento,
    es.cantidad_salidos,
    et.cantidad_total,
    ROUND((es.cantidad_salidos / et.cantidad_total) * 100, 2) AS tasa_rotacion_porcentual
FROM 
    departments d
JOIN 
    empleados_salidos es ON d.dept_no = es.dept_no
JOIN 
    empleados_totales et ON d.dept_no = et.dept_no
ORDER BY 
    tasa_rotacion_porcentual DESC