select department, employee, salary
from (
select d.name as Department, e.name as employee, e.salary,
rank() over (
    partition by d.name
    order by salary desc
) as rnk
from Employee e
Inner Join Department d
ON e.departmentID = d.id
) data
where rnk = 1