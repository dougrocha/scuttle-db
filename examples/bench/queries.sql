-- One query per line. Lines starting with -- are skipped.
SELECT * FROM users_pk WHERE id = 999999
SELECT * FROM users WHERE id = 999999
SELECT * FROM users_pk WHERE id > 900000
SELECT * FROM users_pk WHERE id > 0
SELECT count(*) FROM users WHERE age = 5
SELECT age, count(*) FROM users GROUP BY age
SELECT * FROM users ORDER BY age LIMIT 10
