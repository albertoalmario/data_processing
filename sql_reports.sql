--*************************
--*******Challenge 2*******
--*************************
/* Requirement 1:
Number of employees hired for each job and department in 2021 divided by quarter. The
table must be ordered alphabetically by department and job.
*/
SELECT j.job
, d.department
, SUM(CASE (strftime('%m', he.datetime)/4)+1 WHEN 1 THEN 1 ELSE 0 END) as Q1
, SUM(CASE (strftime('%m', he.datetime)/4)+1 WHEN 2 THEN 1 ELSE 0 END) as Q2
, SUM(CASE (strftime('%m', he.datetime)/4)+1 WHEN 3 THEN 1 ELSE 0 END) as Q3
, SUM(CASE (strftime('%m', he.datetime)/4)+1 WHEN 4 THEN 1 ELSE 0 END) as Q4
FROM hired_employees as he
LEFT JOIN jobs as j
ON he.job_id = j.id 
LEFT JOIN departments as d
ON he.department_id = d.id 
WHERE strftime('%Y', he.datetime) = '2021'
GROUP BY d.department
; 

/* Requirement 2:
List of ids, name and number of employees hired of each department that hired more
employees than the mean of employees hired in 2021 for all the departments, ordered
by the number of employees hired (descending).
*/
SELECT d.id, d.department, COUNT(1) as hired
FROM hired_employees as he
LEFT JOIN jobs as j
ON he.job_id = j.id 
LEFT JOIN departments as d
ON he.department_id = d.id 
WHERE strftime('%Y', he.datetime) = '2021'
GROUP BY d.id, d.department
HAVING COUNT(1) >= (SELECT COUNT(1)/COUNT(DISTINCT(he.department_id)) as hired_avg  FROM hired_employees as he WHERE strftime('%Y', he.datetime) = '2021')
ORDER BY hired DESC
;