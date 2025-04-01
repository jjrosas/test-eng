WITH 
tiempo_permanencia AS (
    SELECT 
        de.emp_no,
        de.dept_no,
        DATEDIFF(
            IF(de.to_date = '9999-01-01', CURDATE(), de.to_date),
            de.from_date
        )/365 AS años_en_dept
    FROM 
        dept_emp de
),
retention_por_gerente AS (
    SELECT 
        dm.emp_no AS manager_id,
        m.first_name AS manager_name,
        m.last_name  as manager_last_name,
        d.dept_name AS department,
        AVG(tp.años_en_dept) AS avg_years_in_dept,
        COUNT(DISTINCT tp.emp_no) AS employee_count
    FROM 
        dept_manager dm
    JOIN 
        employees m ON dm.emp_no = m.emp_no
    JOIN 
        tiempo_permanencia tp ON dm.dept_no = tp.dept_no
    JOIN 
        departments d ON dm.dept_no = d.dept_no
    WHERE 
        tp.años_en_dept > 0  
    GROUP BY 
        dm.emp_no, m.first_name, m.last_name, d.dept_name
)
SELECT 
    manager_id,
    manager_name,
    manager_last_name,
    department,
    ROUND(avg_years_in_dept, 2) AS avg_retention_years,
    employee_count,
    RANK() OVER (ORDER BY avg_years_in_dept DESC) AS retention_rank
FROM 
    retention_por_gerente
ORDER BY 
    retention_rank desc;