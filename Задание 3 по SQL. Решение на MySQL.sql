# Решение апробированно на MySQL 8.0
# 1. Создаем структуру входной таблицы для теста
/*
CREATE TABLE dev_test.posts_stats
(
	dttm    timestamp,
    post_id integer,
    views   integer,
    likes   integer,
    shares  integer,
    primary key (dttm, post_id)
)
*/

# 2. Заполняем входную таблицу данными
/* 
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:00:00'), 1, 100, 10, 1);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:01:00'), 1, 100, 10, 1);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:02:00'), 1, 100, 11, 1);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:04:00'), 1, 100, 11, 1);

INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:00:00'), 2, 200, 20, 2);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:01:00'), 2, 210, 21, 2);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:02:00'), 2, 220, 22, 3);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:03:00'), 2, 220, 21, 3);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:04:00'), 2, 250, 21, 3);

INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:02:00'), 3, 0, 0, 0);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:03:00'), 3, 50, 1, 1);
INSERT INTO  dev_test.posts_stats VALUES (timestamp('2019-01-01 00:04:00'), 3, 70, 5, 2);
*/

#SELECT * FROM dev_test.posts_stats order by post_id, dttm;

# 3. Запрос, трансформирующий исходную таблицу posts_stats в вид SCD Type2
# Примечание: при больших объемах данных JOIN будет медленно работать. Выход: создать копию таблицы и взаимодействовать с ней
# В большинстве других баз данных, можно воспользоваться конструкцией MERGE (insert/update)
# При этом, помнить, что вначале необходимо делать операции UPDATE, потом INSERT для повышения скорости отработки запроса 
# Поиск timestamp полуинтервалов можно осуществить с помощью LAG/LEAD функций внутри окон по post_id (LAG(...) over (partition by ...))
SELECT 
		ps.post_id
      , ps.views
      , ps.likes
      , ps.shares
      , ps.dttm as effective_from
      , CASE
			WHEN MIN(pp.dttm) IS NULL THEN timestamp('9999-12-31 23:59:59')
            ELSE MIN(pp.dttm)
		END as effective_to
FROM dev_test.posts_stats as ps
LEFT JOIN dev_test.posts_stats as pp ON ps.post_id = pp.post_id 
									AND ps.dttm < pp.dttm
									AND ps.views + ps.likes + ps.shares <> pp.views + pp.likes + pp.shares
GROUP BY ps.post_id, ps.views, ps.likes, ps.shares
ORDER BY ps.post_id, ps.dttm;