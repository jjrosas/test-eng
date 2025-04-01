SELECT 
    d.dept_name AS departamento,
    t.title,
    YEAR(s.from_date) AS year_report,
    ROUND(AVG(s.salary), 2) AS avg_salary,
    COUNT(DISTINCT s.emp_no) AS cantidad_empleados
FROM 
    salaries s
JOIN 
    titles t ON s.emp_no = t.emp_no 
    AND s.from_date BETWEEN t.from_date AND t.to_date
JOIN 
    dept_emp de ON s.emp_no = de.emp_no
    AND s.from_date BETWEEN de.from_date AND de.to_date
JOIN 
    departments d ON de.dept_no = d.dept_no
WHERE 
    YEAR(s.from_date) < 9999
GROUP BY 
    d.dept_name, 
    t.title, 
    YEAR(s.from_date)
ORDER BY 
    departamento, 
    t.title,
    year_report
    