import sqlite3
import pandas as pd
from pandas import DataFrame

# Sample data for the emp table
emp_data = {
    "Empno": [7839, 7566, 7698, 7782, 7934, 7654, 7900, 7521],
    "Ename": ["KING", "JONES", "BLAKE", "CLARK", "MILLER", "MARTIN", "JAMES", "WARD"],
    "Job": ["PRESIDENT", "MANAGER", "MANAGER", "MANAGER", "CLERK", "SALESMAN", "CLERK", "SALESMAN"],
    "Mgr": [None, 7839, 7839, 7839, 7782, 7698, 7698, 7698],
    "Hiredate": ["1981-11-17", "1981-04-02", "1981-05-01", "1981-06-09", "1982-01-23", "1981-09-28", "1981-12-03", "1981-02-22"],
    "Sal": [5000, 2975, 2850, 2450, 1300, 1250, 950, 1250],
    "Comm": [None, None, None, None, None, 1400, None, 500],
    "Deptno": [10, 20, 30, 10, 10, 30, 30, 30]
}

# Sample data for the dept table
dept_data = {
    "Deptno": [10, 20, 30],
    "Dname": ["ACCOUNTING", "RESEARCH", "SALES"],
    "Loc": ["NEW YORK", "DALLAS", "CHICAGO"]
}

# Sample data for the salgrade table
salgrade_data = {
    "Grade": [1, 2, 3, 4, 5],
    "Losal": [700, 1200, 1400, 2000, 3000],
    "Hisal": [1200, 1400, 2000, 3000, 5000]
}

# Create DataFrames
emp_df = DataFrame(emp_data)
dept_df = DataFrame(dept_data)
salgrade_df = DataFrame(salgrade_data)

# Connect to SQLite database
conn = sqlite3.connect('emp_data.db')

# Load the data into the database
emp_df.to_sql('emp', conn, if_exists='replace', index=False)
dept_df.to_sql('dept', conn, if_exists='replace', index=False)
salgrade_df.to_sql('salgrade', conn, if_exists='replace', index=False)

# List of queries
queries = [
    "SELECT * FROM emp;",
    "SELECT DISTINCT Job FROM emp;",
    "SELECT * FROM emp ORDER BY Sal ASC;",
    "SELECT * FROM emp ORDER BY Deptno ASC, Job DESC;",
    "SELECT DISTINCT Job FROM emp ORDER BY Job DESC;",
    "SELECT * FROM emp WHERE Empno IN (SELECT Mgr FROM emp);",
    "SELECT * FROM emp WHERE Hiredate < '1981-01-01';",
    "SELECT Empno, Ename, Sal, Sal/30 AS DailySal, 12*Sal AS AnnSal FROM emp ORDER BY AnnSal ASC;",
    "SELECT Empno, Ename, Job, Hiredate, (strftime('%Y', 'now') - strftime('%Y', Hiredate)) AS Exp FROM emp WHERE Empno IN (SELECT Mgr FROM emp);",
    "SELECT Empno, Ename, Sal, (strftime('%Y', 'now') - strftime('%Y', Hiredate)) AS Exp FROM emp WHERE Mgr = 7369;",
    "SELECT * FROM emp WHERE Comm > Sal;",
    "SELECT * FROM emp WHERE Hiredate > '1981-06-30' AND strftime('%Y', Hiredate) = '1981' ORDER BY Job ASC;",
    "SELECT * FROM emp WHERE (Sal/30) > 100;",
    "SELECT * FROM emp WHERE Job IN ('CLERK', 'ANALYST') ORDER BY Job DESC;",
    "SELECT * FROM emp WHERE Hiredate IN ('1981-05-01', '1981-12-03', '1981-12-17', '1980-01-19') ORDER BY Hiredate ASC;",
    "SELECT * FROM emp WHERE Deptno IN (10, 20);",
    "SELECT * FROM emp WHERE strftime('%Y', Hiredate) = '1981';",
    "SELECT * FROM emp WHERE strftime('%Y-%m', Hiredate) = '1980-08';",
    "SELECT * FROM emp WHERE 12*Sal BETWEEN 22000 AND 45000;",
    "SELECT Ename FROM emp WHERE LENGTH(Ename) = 5;",
    "SELECT Ename FROM emp WHERE Ename LIKE 'S%' AND LENGTH(Ename) = 5;",
    "SELECT * FROM emp WHERE LENGTH(Ename) = 4 AND Ename LIKE '__R%;",
    "SELECT * FROM emp WHERE LENGTH(Ename) = 5 AND Ename LIKE 'S%H';",
    "SELECT * FROM emp WHERE strftime('%m', Hiredate) = '01';",
    "SELECT * FROM emp WHERE strftime('%m', Hiredate) LIKE '_a%';",
    "SELECT * FROM emp WHERE LENGTH(Sal) = 4 AND Sal LIKE '%0';",
    "SELECT * FROM emp WHERE Ename LIKE '%LL%';",
    "SELECT * FROM emp WHERE strftime('%Y', Hiredate) LIKE '8%';",
    "SELECT * FROM emp WHERE Deptno NOT IN (20);",
    "SELECT * FROM emp WHERE Job NOT IN ('PRESIDENT', 'MANAGER') ORDER BY Sal ASC;",
    "SELECT * FROM emp WHERE strftime('%Y', Hiredate) NOT IN ('1981');",
    "SELECT * FROM emp WHERE Empno NOT LIKE '78%';",
    "SELECT e.Ename || ' works for ' || m.Ename AS result FROM emp e JOIN emp m ON e.Mgr = m.Empno;",
    "SELECT * FROM emp WHERE strftime('%m', Hiredate) NOT IN ('03');",
    "SELECT * FROM emp WHERE Job = 'CLERK' AND Deptno = 20;",
    "SELECT * FROM emp WHERE strftime('%Y', Hiredate) = '1981' AND Deptno IN (30, 10);",
    "SELECT * FROM emp WHERE Ename = 'SMITH';",
    "SELECT Loc FROM emp e JOIN dept d ON e.Deptno = d.Deptno WHERE e.Ename = 'SMITH';",
    "SELECT * FROM emp e JOIN dept d ON e.Deptno = d.Deptno WHERE d.Dname IN ('ACCOUNTING', 'RESEARCH') ORDER BY e.Deptno ASC;",
    "SELECT e.Empno, e.Ename, e.Sal, d.Dname FROM emp e JOIN dept d ON e.Deptno = d.Deptno WHERE d.Loc IN ('NEW YORK', 'DALLAS') AND e.Empno IN (SELECT Empno FROM emp WHERE Job IN ('MANAGER', 'ANALYST') AND (strftime('%Y', 'now') - strftime('%Y', Hiredate)) > 7 AND Comm IS NULL) ORDER BY d.Loc ASC;",
    "SELECT e.Empno, e.Ename, e.Sal, d.Dname, d.Loc, e.Deptno, e.Job FROM emp e JOIN dept d ON e.Deptno = d.Deptno WHERE (d.Loc = 'CHICAGO' OR d.Dname = 'ACCOUNTING') AND e.Empno IN (SELECT Empno FROM emp WHERE 12*Sal > 28000 AND Sal NOT IN (3000, 2800) AND Job != 'MANAGER' AND (Empno LIKE '__7%' OR Empno LIKE '__8%')) ORDER BY e.Deptno ASC, e.Job DESC;",
    "SELECT * FROM emp e JOIN salgrade s ON e.Sal BETWEEN s.Losal AND s.Hisal ORDER BY Grade ASC;",
    "SELECT * FROM emp e JOIN salgrade s ON e.Sal BETWEEN s.Losal AND s.Hisal WHERE s.Grade IN (2, 3);",
    "SELECT * FROM emp e JOIN salgrade s ON e.Sal BETWEEN s.Losal AND s.Hisal WHERE s.Grade IN (4, 5) AND e.Empno IN (SELECT Empno FROM emp WHERE Job IN ('MANAGER', 'ANALYST'));",
    "SELECT e.Empno, e.Ename, e.Sal, s.Grade, d.Dname, (strftime('%Y', 'now') - strftime('%Y', e.Hiredate)) AS Exp, 12*e.Sal AS AnnSal FROM emp e JOIN dept d ON e.Deptno = d.Deptno JOIN salgrade s ON e.Sal BETWEEN s.Losal AND s.Hisal WHERE e.Deptno IN (10, 20);",
    "SELECT e.Empno, e.Ename, d.Loc, s.Grade FROM emp e JOIN dept d ON e.Deptno = d.Deptno JOIN salgrade s ON e.Sal BETWEEN s.Losal AND s.Hisal WHERE d.Dname NOT LIKE 'OP%' AND d.Dname NOT LIKE '%S' AND Job LIKE '%A%' ORDER BY e.Sal DESC;"
]

# Execute queries and print results
for i, query in enumerate(queries):
    try:
        print(f"Query {i+1}: {query}\n")
        result = pd.read_sql(query, conn)
        print(result)
    except Exception as e:
        print(f"Error executing query {i+1}: {e}")

# Close the connection
conn.close()
