/*
this is an SQL comments eliminator test script
*/
SELECT * FROM TEST;
SELECT /*ignore this*/ * FROM TEST t1/* WHERE /*nested com
ment here */ id = 1*/ /*;*/ JOIN TEST2  t2 ON t1.id = t2.id;

/*
SELECT * FROM DUAL*/

-- single line comment here


INSERT INTO TEST(name) VALUES('whatever');

SELECT 1 AS "TEST     TEST" FROM TEST; -- TODO preserve whitespaces inside double-quoted elements