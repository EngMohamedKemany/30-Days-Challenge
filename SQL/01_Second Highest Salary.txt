## Approach 1 will not work if the answer is null

with CTE AS (
SELECT salary
FROM Employee
ORDER BY salary DESC
LIMIT 2
)

SELECT SALARY AS SecondHighestSalary
FROM CTE
ORDER BY salary
LIMIT 1;

## Approach 2 will work in all cases

SELECT MAX(salary) as SecondHighestSalary 
FROM Employee WHERE salary 
NOT IN (SELECT MAX(salary) FROM Employee);
