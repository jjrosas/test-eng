WITH 
conteo_genero_dept AS (
    SELECT 
        d.dept_name AS departamento,
        e.gender AS genero,
        COUNT(*) AS cantidad,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY d.dept_name) AS porcentaje
    FROM 
        employees e
    JOIN 
        dept_emp de ON e.emp_no = de.emp_no AND de.to_date = '9999-01-01'
    JOIN 
        departments d ON de.dept_no = d.dept_no
    GROUP BY 
        d.dept_name, e.gender
),
disparidad_genero AS (
    SELECT 
        departamento,
        MAX(CASE WHEN genero = 'M' THEN porcentaje END) AS porcentaje_hombres,
        MAX(CASE WHEN genero = 'F' THEN porcentaje END) AS porcentaje_mujeres,
        ABS(MAX(CASE WHEN genero = 'M' THEN porcentaje END) - 
            MAX(CASE WHEN genero = 'F' THEN porcentaje END)) AS diferencia_porcentual
    FROM 
        conteo_genero_dept
    GROUP BY 
        departamento
)
SELECT 
    departamento,
    ROUND(porcentaje_hombres, 2) AS "% Hombres",
    ROUND(porcentaje_mujeres, 2) AS "% Mujeres",
    ROUND(diferencia_porcentual, 2) AS "Diferencia %",
    CASE 
        WHEN porcentaje_hombres > porcentaje_mujeres THEN 'Masculino'
        ELSE 'Femenino'
    END AS "Género predominante",
    RANK() OVER (ORDER BY diferencia_porcentual DESC) AS "Ranking Disparidad"
FROM 
    disparidad_genero
ORDER BY 
    diferencia_porcentual DESC;