import sqlite3
import pandas as pd
from pandas import DataFrame

# Sample data for the emp table
emp_data = {
    "Empno": [7839, 7566, 7698, 7782, 7934, 7654, 7900, 7521],
    "Ename": ["KING", "JONES", "BLAKE", "CLARK", "MILLER", "MARTIN", "JAMES", "WARD"],
    "Job": ["PRESIDENT", "MANAGER", "MANAGER", "MANAGER", "CLERK", "SALESMAN", "CLERK", "SALESMAN"],
    "Mgr": [None, 7839, 7839, 7839, 7782, 7698, 7698, 7698],
    "Hiredate": ["1981-11-17", "1981-04-02", "1981-05-01", "1981-06-09", "1982-01-23", "1981-09-28", "1981-12-03",
                 "1981-02-22"],
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
    "SELECT e.Empno, e.Ename, d.Loc, s.Grade FROM emp e JOIN dept d ON e.Deptno = d.Deptno JOIN salgrade s ON e.Sal BETWEEN s.Losal AND s.Hisal WHERE d.Dname NOT LIKE 'OP%' AND d.Dname NOT LIKE '%S' AND Job LIKE '%A%' ORDER BY e.Sal DESC;",
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
    "SELECT * FROM emp WHERE LENGTH(Ename) = 4 AND Ename LIKE '__R%';",
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
    "SELECT e.Empno, e.Ename, d.Loc, s.Grade FROM emp e JOIN dept d ON e.Deptno = d.Deptno JOIN salgrade s ON e.Sal BETWEEN s.Losal AND s.Hisal WHERE d.Dname NOT LIKE 'OP%' AND d.Dname NOT LIKE '%S' AND Job LIKE '%A%' ORDER BY e.SalDESC;",
    "SELECT * FROM emp WHERE sal > (SELECT sal FROM emp WHERE ename = 'BLAKE');",
    "SELECT * FROM emp WHERE job = (SELECT job FROM emp WHERE ename = 'ALLEN');",
    "SELECT * FROM emp WHERE hiredate < (SELECT hiredate FROM emp WHERE ename = 'KING');",
    "SELECT * FROM emp w, emp m WHERE w.mgr = m.empno AND w.hiredate < m.hiredate;",
    "SELECT * FROM emp e ,dept d WHERE d.deptno = 20 AND e.deptno = d.deptno AND e.job IN (SELECT e.job FROM emp e, dept d WHERE e.deptno = d.deptno AND d.deptno = 10);",
    "SELECT * FROM emp WHERE sal IN (SELECT sal FROM emp WHERE ename IN ('SMITH', 'FORD')) ORDER BY sal DESC;",
    "SELECT * FROM emp WHERE job = (SELECT job FROM emp WHERE ename = 'MILLER') OR sal > (SELECT sal FROM emp WHERE ename = 'ALLEN');",
    "SELECT * FROM emp WHERE sal > (SELECT sum(ifnull(sal + comm, sal)) FROM emp WHERE job = 'SALESMAN');",
    "SELECT * FROM emp e , dept d WHERE d.loc IN ('CHICAGO', 'BOSTON') AND e.deptno = d.deptno AND e.hiredate < (SELECT hiredate FROM emp WHERE ename = 'BLAKE');",
    "SELECT * FROM emp e WHERE e.deptno IN (SELECT d.deptno FROM dept d WHERE d.dname IN ('ACCOUNTING', 'RESEARCH')) AND e.sal > (SELECT sal FROM emp WHERE ename = 'ALLEN') AND e.hiredate < (SELECT hiredate FROM emp WHERE ename = 'SMITH') AND e.empno IN (SELECT empno FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal AND s.grade IN (3, 4)) ORDER BY e.hiredate DESC;",
    "SELECT * FROM emp WHERE job IN (SELECT job FROM emp WHERE ename IN ('SMITH', 'ALLEN'));",
    "SELECT * FROM emp WHERE sal > (SELECT max(sal) FROM emp);",
    "SELECT * FROM emp WHERE sal IN (SELECT max(sal) FROM emp);",
    "SELECT * FROM emp WHERE sal IN (SELECT max(sal) FROM emp WHERE deptno IN (SELECT deptno FROM dept WHERE dname = 'SALES'));",
    "SELECT * FROM emp WHERE hiredate = (SELECT max(hiredate) FROM emp WHERE empno IN (SELECT empno FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal AND s.grade = 3));",
    "SELECT * FROM emp WHERE hiredate < (SELECT max(hiredate) FROM emp WHERE mgr IN (SELECT empno FROM emp WHERE ename = 'KING'));",
    "SELECT * FROM emp WHERE hiredate IN (SELECT min(hiredate) FROM emp WHERE strftime('%Y', hiredate) = '1981');",
    "SELECT * FROM emp WHERE job IN (SELECT job FROM emp WHERE hiredate IN (SELECT min(hiredate) FROM emp WHERE strftime('%Y', hiredate) = '1981'));",
    "SELECT * FROM emp WHERE hiredate IN (SELECT min(hiredate) FROM emp WHERE empno IN (SELECT empno FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal AND s.grade IN (4, 5))) AND mgr IN (SELECT empno FROM emp WHERE ename = 'KING');",
    "SELECT sum(sal) FROM emp WHERE job = 'MANAGER';",
    "SELECT sum(sal) FROM emp WHERE empno IN (SELECT mgr FROM emp);",
    "SELECT job, sum(12*sal) FROM emp WHERE strftime('%Y', hiredate) = '1981' GROUP BY job;",
    "SELECT sum(sal) FROM emp WHERE empno IN (SELECT empno FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal AND s.grade = 3);",
    "SELECT avg(sal) FROM emp WHERE job = 'CLERK';",
    "SELECT * FROM emp WHERE deptno = 20 AND sal > (SELECT avg(sal) FROM emp WHERE deptno = 10);",
    "SELECT deptno, job, count(*) FROM emp GROUP BY deptno, job;",
    "SELECT w.mgr, count(*) FROM emp w, emp m WHERE w.mgr = m.empno GROUP BY w.mgr ORDER BY w.mgr ASC;",
    "SELECT deptno, count(*) FROM emp GROUP BY deptno HAVING count(*) >= 2;",
    "SELECT s.grade, count(*), max(sal) FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal GROUP BY s.grade;",
    "SELECT d.dname, s.grade, count(*) FROM emp e, dept d, salgrade s WHERE e.deptno = d.deptno AND e.job = 'CLERK' AND e.sal BETWEEN s.losal AND s.hisal GROUP BY d.dname, s.grade HAVING count(*) >= 2;",
    "SELECT * FROM dept WHERE deptno IN (SELECT deptno FROM emp GROUP BY deptno HAVING count(*) = (SELECT max(count(*)) FROM emp GROUP BY deptno));",
    "SELECT * FROM emp WHERE mgr IN (SELECT empno FROM emp WHERE ename = 'JONES');",
    "SELECT * FROM emp WHERE (1.2*sal) > 3000;",
    "SELECT e.empno, e.ename, e.job, e.mgr, e.hiredate, e.sal, e.comm, e.deptno, d.dname FROM emp e, dept d WHERE e.deptno = d.deptno;",
    "SELECT * FROM emp WHERE deptno NOT IN (SELECT deptno FROM dept WHERE dname = 'SALES');",
    "SELECT e.ename, e.deptno, e.sal, e.comm FROM emp e, dept d WHERE e.deptno = d.deptno AND d.loc = 'CHICAGO' AND e.sal BETWEEN 2000 AND 5000;",
    "SELECT * FROM emp w, emp m WHERE w.mgr = m.empno AND w.sal > m.sal;",
    "SELECT s.grade, e.ename FROM emp e, salgrade s WHERE e.deptno IN (10, 20) AND hiredate < '1982-12-31' AND e.sal BETWEEN s.losal AND s.hisal AND s.grade NOT IN (4);",
    "SELECT e.ename, e.job, d.dname, d.loc FROM emp e, dept d WHERE e.deptno = d.deptno AND e.empno IN (SELECT mgr FROM emp);",
    "SELECT w.empno, w.ename, w.job, w.mgr, w.hiredate, w.sal, w.deptno, m.ename AS ManagerName FROM emp w, emp m WHERE w.mgr = m.empno AND m.ename = 'JONES';",
    "SELECT e.ename, e.sal FROM emp e, salgrade s WHERE e.ename = 'FORD' AND e.sal BETWEEN s.losal AND s.hisal AND e.sal = s.hisal;",
    "SELECT e.ename, e.job, d.dname, e.sal, s.grade FROM emp e, dept d, salgrade s WHERE e.deptno = d.deptno AND e.sal BETWEEN s.losal AND s.hisal ORDER BY e.deptno;",
    "SELECT e.ename, e.job, e.sal, s.grade, d.dname FROM emp e, dept d, salgrade s WHERE e.deptno = d.deptno AND e.sal BETWEEN s.losal AND s.hisal AND e.job NOT IN ('CLERK') ORDER BY e.sal DESC;",
    "SELECT e.ename, e.job FROM emp e WHERE mgr IS NULL;",
    "SELECT e.ename, e.deptno FROM emp e WHERE e.sal IN (SELECT max(sal) FROM emp GROUP BY deptno);",
    "SELECT * FROM emp WHERE sal = (SELECT (max(sal) + min(sal))/2 FROM emp);",
    "SELECT deptno, count(*) FROM emp GROUP BY deptno HAVING count(*) < 3;",
    "SELECT d.dname, count(*) FROM emp e, dept d WHERE e.deptno = d.deptno GROUP BY d.dname HAVING count(*) >= 3;",
    "SELECT * FROM emp m WHERE m.empno IN (SELECT mgr FROM emp) AND m.sal > (SELECT avg(e.sal) FROM emp e WHERE e.mgr = m.empno);",
    "SELECT e.ename, e.sal, e.comm FROM emp e WHERE ifnull(e.sal + e.comm, e.sal) >= any (SELECT sal FROM emp);",
    "SELECT distinct W.empno, W.ename, W.sal FROM (SELECT w.empno, w.ename, w.sal FROM emp w, emp m WHERE w.mgr = m.empno AND w.sal < m.sal) W, (SELECT * FROM emp WHERE empno IN (SELECT mgr FROM emp)) A WHERE W.sal > A.sal;",
    "SELECT d.deptno, avg(ifnull(e1.comm, e1.sal + e1.comm, e1.sal)) avg, e2.ename FROM emp e1, emp e2, dept d WHERE d.deptno = e1.deptno AND d.deptno = e2.deptno GROUP BY d.deptno, e2.ename;",
    "SELECT * FROM emp WHERE 5 > (SELECT count(*) FROM emp WHERE e.sal > sal);",
    "SELECT * FROM emp w, emp m WHERE w.mgr = m.empno AND w.sal > m.sal;",
    "SELECT * FROM emp WHERE empno IN (SELECT mgr FROM emp) AND mgr NOT IN (SELECT empno FROM emp WHERE job = 'PRESIDENT');",
    "SELECT * FROM emp WHERE deptno NOT IN (SELECT deptno FROM dept);",
    "SELECT e.ename, e.sal, e.comm, ifnull(comm, sal + comm, sal) NETPAY FROM emp e WHERE ifnull(comm, sal + comm, sal) > any (SELECT sal FROM emp WHERE empno = e.empno);",
    "SELECT ename FROM emp WHERE add_months(hiredate, 240) > '1989-12-31';",
    "SELECT * FROM emp WHERE mod(sal, 2) = 1;",
    "SELECT * FROM emp WHERE length(sal) = 3;",
    "SELECT * FROM emp WHERE strftime('%m', hiredate) = '12';",
    "SELECT * FROM emp WHERE ename LIKE '%A%';",
    "SELECT * FROM emp WHERE instr(sal, deptno) > 0;",
    "SELECT * FROM emp WHERE substr(hiredate, 1, 2) = substr(sal, length(sal) - 1, length(sal));",
    "SELECT * FROM emp WHERE strftime('%y', hiredate) IN (SELECT 0.1 * sal FROM emp);",
    "SELECT lower(substr(ename, 1, round(length(ename) / 2))) || substr(ename, round(length(ename) / 2) + 1, length(ename)) FROM emp;",
    "SELECT * FROM dept d WHERE length(dname) IN (SELECT count(*) FROM emp e WHERE e.deptno = d.deptno);",
    "SELECT * FROM emp WHERE strftime('%d', hiredate) < '15';",
    "SELECT * FROM dept d WHERE length(dname) IN (SELECT count(*) FROM emp WHERE d.deptno <> deptno GROUP BY deptno);",
    "SELECT * FROM emp WHERE job = 'MANAGER';",
    "SELECT * FROM dept WHERE deptno IN (SELECT deptno FROM emp GROUP BY deptno HAVING count(*) = (SELECT max(count(*)) FROM emp GROUP BY deptno));",
    "SELECT count(*) FROM (SELECT * FROM emp MINUS SELECT * FROM emp WHERE job != 'MANAGER');",
    "SELECT * FROM emp WHERE hiredate IN (SELECT hiredate FROM emp WHERE empno <> empno);",
    "SELECT * FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal AND s.grade = 0.1 * (SELECT deptno FROM dept WHERE dname = 'SALES');",
    "SELECT d.dname FROM dept d, emp e WHERE e.deptno = d.deptno GROUP BY d.dname HAVING count(*) > (SELECT avg(count(*)) FROM emp GROUP BY deptno);",
    "SELECT m.ename, count(*) FROM emp w, emp m WHERE w.mgr = m.empno GROUP BY m.ename HAVING count(*) = (SELECT max(count(*)) FROM emp GROUP BY mgr);",
    "SELECT ename, to_char(1.15 * sal, '$99,999') AS 'SAL' FROM emp;",
    "SELECT ename || job AS 'EMP_AND_JOB' FROM emp;",
    "SELECT ename || ' (' || lower(job) || ')' AS 'EMPLOYEE' FROM emp;",
    "SELECT empno, ename, sal, to_char(hiredate, 'MONTH DD, YYYY') FROM emp;",
    "SELECT empno, ename, sal || ' JUST SALARY' AS 'SAL' FROM emp WHERE sal > 1500 UNION SELECT empno, ename, sal || ' ON TARGET' AS 'SAL' FROM emp WHERE sal = 1500 UNION SELECT empno, ename, sal || ' BELOW 1500' AS 'SAL' FROM emp WHERE sal < 1500;",
    "SELECT to_char(to_date('DD-MM-YY', 'dd-mm-yy'), 'day') FROM dual;",
    "DEFINE service = ((months_between(sysdate, hiredate)) / 12); SELECT empno, ename, &service FROM emp WHERE ename = '&name';",
    "SELECT * FROM emp WHERE hiredate IN (SELECT min(hiredate) FROM emp WHERE strftime('%Y', hiredate) = '1981');",
    "SELECT * FROM emp WHERE hiredate = (SELECT min(hiredate) FROM emp WHERE strftime('%Y', hiredate) = '1981');",
    "SELECT * FROM emp WHERE job IN (SELECT job FROM emp WHERE hiredate IN (SELECT min(hiredate) FROM emp WHERE strftime('%Y', hiredate) = '1981'));",
    "SELECT * FROM emp WHERE hiredate IN (SELECT min(hiredate) FROM emp WHERE empno IN (SELECT empno FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal AND s.grade IN (4, 5))) AND mgr IN (SELECT empno FROM emp WHERE ename = 'KING');",
    "SELECT sum(sal) FROM emp WHERE job = 'MANAGER';",
    "SELECT sum(sal) FROM emp WHERE empno IN (SELECT mgr FROM emp);",
    "SELECT job, sum(12*sal) FROM emp WHERE strftime('%Y', hiredate) = '1981' GROUP BY job;",
    "SELECT sum(sal) FROM emp WHERE empno IN (SELECT empno FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal AND s.grade = 3);",
    "SELECT avg(sal) FROM emp WHERE job = 'CLERK';",
    "SELECT * FROM emp WHERE deptno = 20 AND sal > (SELECT avg(sal) FROM emp WHERE deptno = 10);",
    "SELECT deptno, job, count(*) FROM emp GROUP BY deptno, job;",
    "SELECT w.mgr, count(*) FROM emp w, emp m WHERE w.mgr = m.empno GROUP BY w.mgr ORDER BY w.mgr ASC;",
    "SELECT deptno, count(*) FROM emp GROUP BY deptno HAVING count(*) >= 2;",
    "SELECT s.grade, count(*), max(sal) FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal GROUP BY s.grade;",
    "SELECT d.dname, s.grade, count(*) FROM emp e, dept d, salgrade s WHERE e.deptno = d.deptno AND e.job = 'CLERK' AND e.sal BETWEEN s.losal AND s.hisal GROUP BY d.dname, s.grade HAVING count(*) >= 2;",
    "SELECT * FROM dept WHERE deptno IN (SELECT deptno FROM emp GROUP BY deptno HAVING count(*) = (SELECT max(count(*)) FROM emp GROUP BY deptno));",
    "SELECT * FROM emp WHERE mgr IN (SELECT empno FROM emp WHERE ename = 'JONES');",
    "SELECT * FROM emp WHERE (1.2*sal) > 3000;",
    "SELECT e.empno, e.ename, e.job, e.mgr, e.hiredate, e.sal, e.comm, e.deptno, d.dname FROM emp e, dept d WHERE e.deptno = d.deptno;",
    "SELECT * FROM emp WHERE deptno NOT IN (SELECT deptno FROM dept WHERE dname = 'SALES');",
    "SELECT e.ename, e.deptno, e.sal, e.comm FROM emp e, dept d WHERE e.deptno = d.deptno AND d.loc = 'CHICAGO' AND e.sal BETWEEN 2000 AND 5000;",
    "SELECT * FROM emp w, emp m WHERE w.mgr = m.empno AND w.sal > m.sal;",
    "SELECT s.grade, e.ename FROM emp e, salgrade s WHERE e.deptno IN (10, 20) AND hiredate < '1982-12-31' AND e.sal BETWEEN s.losal AND s.hisal AND s.grade NOT IN (4);",
    "SELECT e.ename, e.job, d.dname, d.loc FROM emp e, dept d WHERE e.deptno = d.deptno AND e.empno IN (SELECT mgr FROM emp);",
    "SELECT w.empno, w.ename, w.job, w.mgr, w.hiredate, w.sal, w.deptno, m.ename AS ManagerName FROM emp w, emp m WHERE w.mgr = m.empno AND m.ename = 'JONES';",
    "SELECT e.ename, e.sal FROM emp e, salgrade s WHERE e.ename = 'FORD' AND e.sal BETWEEN s.losal AND s.hisal AND e.sal = s.hisal;",
    "SELECT e.ename, e.job, d.dname, e.sal, s.grade FROM emp e, dept d, salgrade s WHERE e.deptno = d.deptno AND e.sal BETWEEN s.losal AND s.hisal ORDER BY e.deptno;",
    "SELECT e.ename, e.job, e.sal, s.grade, d.dname FROM emp e, dept d, salgrade s WHERE e.deptno = d.deptno AND e.sal BETWEEN s.losal AND s.hisal AND e.job NOT IN ('CLERK') ORDER BY e.sal DESC;",
    "SELECT e.ename, e.jobFROM emp e WHERE mgr IS NULL;",
    "SELECT e.ename, e.deptno FROM emp e WHERE e.sal IN (SELECT max(sal) FROM emp GROUP BY deptno);",
    "SELECT * FROM emp WHERE sal = (SELECT (max(sal) + min(sal))/2 FROM emp);",
    "SELECT deptno, count(*) FROM emp GROUP BY deptno HAVING count(*) < 3;",
    "SELECT d.dname, count(*) FROM emp e, dept d WHERE e.deptno = d.deptno GROUP BY d.dname HAVING count(*) >= 3;",
    "SELECT * FROM emp m WHERE m.empno IN (SELECT mgr FROM emp) AND m.sal > (SELECT avg(e.sal) FROM emp e WHERE e.mgr = m.empno);",
    "SELECT e.ename, e.sal, e.comm FROM emp e WHERE ifnull(e.sal + e.comm, e.sal) >= any (SELECT sal FROM emp);",
    "SELECT distinct W.empno, W.ename, W.sal FROM (SELECT w.empno, w.ename, w.sal FROM emp w, emp m WHERE w.mgr = m.empno AND w.sal < m.sal) W, (SELECT * FROM emp WHERE empno IN (SELECT mgr FROM emp)) A WHERE W.sal > A.sal;",
    "SELECT d.deptno, avg(ifnull(e1.comm, e1.sal + e1.comm, e1.sal)) avg, e2.ename FROM emp e1, emp e2, dept d WHERE d.deptno = e1.deptno AND d.deptno = e2.deptno GROUP BY d.deptno, e2.ename;",
    "SELECT * FROM emp WHERE 5 > (SELECT count(*) FROM emp WHERE e.sal > sal);",
    "SELECT * FROM emp w, emp m WHERE w.mgr = m.empno AND w.sal > m.sal;",
    "SELECT * FROM emp WHERE empno IN (SELECT mgr FROM emp) AND mgr NOT IN (SELECT empno FROM emp WHERE job = 'PRESIDENT');",
    "SELECT * FROM emp WHERE deptno NOT IN (SELECT deptno FROM dept);",
    "SELECT e.ename, e.sal, e.comm, ifnull(comm, sal + comm, sal) NETPAY FROM emp e WHERE ifnull(comm, sal + comm, sal) > any (SELECT sal FROM emp WHERE empno = e.empno);",
    "SELECT ename FROM emp WHERE add_months(hiredate, 240) > '1989-12-31';",
    "SELECT * FROM emp WHERE mod(sal, 2) = 1;",
    "SELECT * FROM emp WHERE length(sal) = 3;",
    "SELECT * FROM emp WHERE strftime('%m', hiredate) = '12';",
    "SELECT * FROM emp WHERE ename LIKE '%A%';",
    "SELECT * FROM emp WHERE instr(sal, deptno) > 0;",
    "SELECT * FROM emp WHERE substr(hiredate, 1, 2) = substr(sal, length(sal) - 1, length(sal));",
    "SELECT * FROM emp WHERE strftime('%y', hiredate) IN (SELECT 0.1 * sal FROM emp);",
    "SELECT lower(substr(ename, 1, round(length(ename) / 2))) || substr(ename, round(length(ename) / 2) + 1, length(ename)) FROM emp;",
    "SELECT * FROM dept d WHERE length(dname) IN (SELECT count(*) FROM emp e WHERE e.deptno = d.deptno);",
    "SELECT * FROM emp WHERE strftime('%d', hiredate) < '15';",
    "SELECT * FROM dept d WHERE length(dname) IN (SELECT count(*) FROM emp WHERE d.deptno <> deptno GROUP BY deptno);",
    "SELECT * FROM emp WHERE job = 'MANAGER';",
    "SELECT * FROM dept WHERE deptno IN (SELECT deptno FROM emp GROUP BY deptno HAVING count(*) = (SELECT max(count(*)) FROM emp GROUP BY deptno));",
    "SELECT count(*) FROM (SELECT * FROM emp MINUS SELECT * FROM emp WHERE job != 'MANAGER');",
    "SELECT * FROM emp WHERE hiredate IN (SELECT hiredate FROM emp WHERE empno <> empno);",
    "SELECT * FROM emp e, salgrade s WHERE e.sal BETWEEN s.losal AND s.hisal AND s.grade = 0.1 * (SELECT deptno FROM dept WHERE dname = 'SALES');",
    "SELECT d.dname FROM dept d, emp e WHERE e.deptno = d.deptno GROUP BY d.dname HAVING count(*) > (SELECT avg(count(*)) FROM emp GROUP BY deptno);",
    "SELECT m.ename, count(*) FROM emp w, emp m WHERE w.mgr = m.empno GROUP BY m.ename HAVING count(*) = (SELECT max(count(*) FROM emp GROUP BY mgr);",
    "SELECT ename, to_char(1.15 * sal, '$99,999') AS 'SAL' FROM emp;",
    "SELECT ename || job AS 'EMP_AND_JOB' FROM emp;",
    "SELECT ename || ' (' || lower(job) || ')' AS 'EMPLOYEE' FROM emp;",
    "SELECT empno, ename, sal, to_char(hiredate, 'MONTH DD, YYYY') FROM emp;",
    "SELECT empno, ename, sal || ' JUST SALARY' AS 'SAL' FROM emp WHERE sal > 1500 UNION SELECT empno, ename, sal || ' ON TARGET' AS 'SAL' FROM emp WHERE sal = 1500 UNION SELECT empno, ename, sal || ' BELOW 1500' AS 'SAL' FROM emp WHERE sal < 1500;",
    "SELECT to_char(to_date('DD-MM-YY', 'dd-mm-yy'), 'day') FROM dual;",
    "DEFINE service = ((months_between(sysdate, hiredate)) / 12); SELECT empno, ename, &service FROM emp WHERE ename = '&name';",
    "SELECT empno, ename, hiredate, next_day(last_day(hiredate),'FRIDAY')-7 FROM emp WHERE strftime('%d', hiredate) <= 15 UNION SELECT empno, ename, hiredate, next_day(last_day(hiredate),'FRIDAY') FROM emp WHERE strftime('%d', hiredate) > 15;",
    "SELECT length(replace(ename, ' ', '')) FROM emp;",
    "SELECT * FROM emp WHERE instr(sal,'.',1,1) > 0;",
    "SELECT * FROM emp WHERE instr(to_char(sal, 9999), deptno, 1, 1) > 0 AND instr(to_char(sal, 9999), deptno, 1, 2) > 0;",
    "SELECT distinct m.ename, m.sal FROM emp w, emp m WHERE w.mgr = m.empno AND w.sal > m.sal;",
    "SELECT * FROM emp w WHERE sal < any (SELECT sal FROM emp WHERE w.empno = mgr);",
    "SELECT * FROM emp w WHERE empno IN (SELECT mgr FROM emp WHERE w.sal < sal);",
    "SELECT * FROM emp WHERE mgr IN (SELECT empno FROM emp WHERE ename = 'BLAKE');",
    "SELECT * FROM emp WHERE empno IN (SELECT mgr FROM emp);",
    "SELECT * FROM emp WHERE mgr IN (SELECT empno FROM emp WHERE ename = 'JONES');",
    "SELECT e.ename, w.ename, m.ename FROM emp e, emp w, emp m WHERE e.mgr = w.empno AND w.ename = 'JONES' AND w.mgr = m.empno;",
    "SELECT * FROM emp WHERE sal > 30000;",
    "SELECT count(*) FROM emp WHERE job = 'MANAGER';",
    "SELECT avg(sal), avg(sal+nvl(comm,0)) FROM emp;",
    "SELECT empno, count(*) FROM emp GROUP BY empno;",
    "SELECT * FROM emp WHERE sal < 1000 ORDER BY sal;",
    "SELECT e.ename, e.job, 12*e.sal AS 'ANNUALSALARY', e.deptno, d.dname, s.grade FROM emp e, dept d, salgrade s WHERE e.deptno = d.deptno AND e.sal BETWEEN s.losal AND s.hisal AND (12*e.sal) >= 36000 OR e.job != 'CLERK';",
    "SELECT * FROM emp WHERE strftime('%Y', hiredate) = '1984' AND job IN (SELECT job FROM emp WHERE strftime('%Y', hiredate) = '1983');",
    "SELECT * FROM emp WHERE hiredate < (SELECT hiredate FROM emp WHERE empno = e.mgr);",
    "SELECT * FROM emp w, emp m WHERE w.empno = m.empno AND m.ename = 'KING' ORDER BY e.sal DESC;",
    "SELECT * FROM emp WHERE sal IN (SELECT max(sal) FROM emp GROUP BY job);",
    "SELECT * FROM emp WHERE hiredate IN (SELECT max(hiredate) FROM emp WHERE e.deptno = deptno);",
    "SELECT * FROM emp WHERE sal > (SELECT avg(sal) FROM emp WHERE e.deptno = deptno);",
    "SELECT deptno FROM emp GROUP BY deptno HAVING count(*) = 0;",
    "SELECT count(*), avg(sal), deptno, job FROM emp GROUP BY deptno, job;",
    "SELECT * FROM emp WHERE sal = (SELECT max(sal) FROM emp) AND comm IS NOT NULL;",
    "SELECT * FROM emp WHERE deptno != 10 AND job IN (SELECT job FROM emp WHERE deptno = 10) AND sal IN (SELECT sal FROM emp WHERE deptno = 10);",
    "SELECT deptno, name, job, sal, sal+nvl(comm,0) FROM emp WHERE job = 'SALESMAN' AND sal IN (SELECT max(sal+nvl(comm,0)) FROM emp WHERE comm IS NOT NULL) ORDER BY sal + nvl(comm,0) DESC;",
    "SELECT deptno, ename, sal, job, sal+nvl(comm,0) FROM emp e WHERE 2 = (SELECT count(distinct sal+nvl(comm,0)) FROM emp WHERE (e.sal+nvl(comm,0)) < (sal+nvl(comm,0));",
    "SELECT deptno, avg(sal) FROM emp GROUP BY deptno HAVING avg(sal) < (SELECT avg(sal) FROM emp);",
    "SELECT w.ename, w.sal, m.ename, m.sal FROM emp w, emp m WHERE w.mgr = m.empno AND w.sal > m.sal;",
    "SELECT * FROM emp WHERE deptno IN (SELECT deptno FROM emp e HAVING avg(sal) = (SELECT max(avg(sal)) FROM emp GROUP BY deptno) GROUP BY deptno);",
    "SELECT empno, sal, comm FROM emp;",
    "SELECT * FROM emp ORDER BY sal ASC;",
    "SELECT * FROM emp e ORDER BY e.job ASC, e.empno DESC;",
    "SELECT unique deptno FROM emp;",
    "SELECT unique deptno, job FROM emp;",
    "SELECT * FROM emp WHERE ename = 'BLAKE';",
    "SELECT * FROM emp WHERE job = 'CLERK';",
    "SELECT * FROM emp WHERE hiredate = '1981-05-01';",
    "SELECT e.empno, e.ename, e.sal, e.deptno FROM emp WHERE e.deptno = 10 ORDER BY e.sal ASC;",
    "SELECT * FROM emp WHERE sal < 3500;",
    "SELECT e.empno, e.ename, e.sal FROM emp WHERE hiredate < '1981-04-01';",
    "SELECT * FROM emp WHERE 12*sal < 25000 ORDER BY sal ASC;",
    "SELECT e.empno, e.ename, 12*sal AS 'ANN SAL', (12*sal)/365 AS 'DAILY SAL' FROM emp WHERE e.job = 'SALESMAN' ORDER BY 'ANN SAL' ASC;",
    "SELECT empno, ename, hiredate, (SELECT sysdate FROM dual), (months_between(sysdate, hiredate)/12) AS 'EXP' FROM emp ORDER BY 'EXP' ASC;",
    "SELECT * FROM emp WHERE (months_between(sysdate, hiredate)/12) > 10;",
    "SELECT e.empno, e.ename, e.sal, 0.3*e.sal AS 'TA30%', 0.4*e.sal AS 'DA40%', 0.5*e.sal AS 'HRA50%', (0.3*e.sal + 0.4*e.sal + 0.5*e.sal) AS 'GROSS', 0.1*e.sal AS 'LIC', 0.1*e.sal AS 'PF', ((0.3*e.sal + 0.4*e.sal + 0.5*e.sal) - (0.1*e.sal + 0.1*e.sal)) AS 'NET' FROM emp ORDER BY 'NET' ASC;",
    "SELECT * FROM emp WHERE job = 'MANAGER';",
    "SELECT * FROM emp WHERE job IN ('CLERK', 'MANAGER');",
    "SELECT * FROM emp WHERE hiredate IN ('1981-05-01', '1981-11-17', '1981-12-30');",
    "SELECT * FROM emp WHERE strftime('%Y', hiredate) = '1981';",
    "SELECT * FROM emp WHERE 12*sal BETWEEN 23000 AND 40000;",
    "SELECT * FROM emp WHERE mgr IN (7369, 7890, 7654, 7900);",
    "SELECT * FROM emp WHERE hiredate BETWEEN '1982-07-01' AND '1982-12-31';",
    "SELECT * FROM emp WHERE length(ename) = 4;",
    "SELECT * FROM emp WHERE ename LIKE 'M%' AND length(ename) = 5;",
    "SELECT * FROM emp WHERE ename LIKE '%H' AND length(ename) = 5;",
    "SELECT * FROM emp WHERE ename LIKE 'M%';",
    "SELECT * FROM emp WHERE strftime('%y', hiredate) = '81';",
    "SELECT * FROM emp WHERE sal LIKE '%00';",
    "SELECT * FROM emp WHERE strftime('%m', hiredate) = '01';",
    "SELECT * FROM emp WHERE strftime('%m', hiredate) LIKE '_a%';",
    "SELECT * FROM emp WHERE length(sal) = 4;",
    "SELECT * FROM emp WHERE strftime('%Y', hiredate) BETWEEN '1980' AND '1989';",
    "SELECT * FROM emp WHERE job = 'CLERK' AND (months_between(sysdate, hiredate)/12) > 8;",
    "SELECT * FROM emp WHERE job = 'MANAGER' AND deptno IN (10, 20);",
    "SELECT * FROM emp WHERE strftime('%m', hiredate) = '01' AND sal BETWEEN 1500 AND 4000;",
    "SELECT distinct job FROM emp WHERE deptno IN (20, 30) ORDER BY job DESC;",
    "SELECT * FROM emp WHERE (mgr LIKE '7%' AND mgr NOT LIKE '%9%') AND strftime('%Y', hiredate) < '1983';",
    "SELECT * FROM emp WHERE job IN ('MANAGER', 'ANALYST') AND sal BETWEEN 2000 AND 5000 AND comm IS NULL;",
    "SELECT empno, ename, sal, job FROM emp WHERE 12 * (sal + COALESCE(comm, 0)) < 34000 AND comm IS NOT NULL AND comm < sal AND job = 'SALESMAN' AND deptno = 30;",

    "SELECT empno, ename, sal, job FROM emp WHERE 12*(sal + IFNULL(comm, 0)) < 34000 AND comm IS NOT NULL AND comm < sal AND job = 'SALESMAN' AND deptno = 30;"


]

# Execute queries and print results
for i, query in enumerate(queries):
    try:
        print(f"Query {i + 1}: {query}\n")
        result = pd.read_sql(query, conn)
        print(result)
    except Exception as e:
        print(f"Error executing query {i + 1}: {e}")

# Close the connection
conn.close()
