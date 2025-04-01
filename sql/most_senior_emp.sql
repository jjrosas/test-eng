SELECT 
    e.emp_no,
    e.first_name,
    e.last_name,
    e.hire_date,
    ROUND(DATEDIFF(CURDATE(), e.hire_date) / 365, 2) AS seniority,
    t.title AS actual_title,
    d.dept_name AS actual_dept
FROM 
    employees e
JOIN 
    (SELECT emp_no, title, from_date 
     FROM titles t1
     WHERE from_date = (SELECT MAX(from_date) 
                       FROM titles t2 
                       WHERE t2.emp_no = t1.emp_no)) t ON e.emp_no = t.emp_no
JOIN 
    (SELECT emp_no, dept_no, from_date 
     FROM dept_emp de1
     WHERE from_date = (SELECT MAX(from_date) 
                       FROM dept_emp de2 
                       WHERE de2.emp_no = de1.emp_no)) de ON e.emp_no = de.emp_no
JOIN 
    departments d ON de.dept_no = d.dept_no
WHERE 
    EXISTS (SELECT 1 FROM dept_emp de WHERE de.emp_no = e.emp_no AND de.to_date = '9999-01-01')
ORDER BY 
    seniority DESC
LIMIT 10;