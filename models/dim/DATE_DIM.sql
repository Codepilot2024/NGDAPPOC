{{ config(
	    materialized="table",
	    schema='DIM'
	           ) }}
select * from (
SELECT 
    ROW_NUMBER() OVER (ORDER BY Date) AS DateID,
    Date,
    EXTRACT(DAY FROM Date) AS Day,
    EXTRACT(MONTH FROM Date) AS Month,
    EXTRACT(QUARTER FROM Date) AS Quarter,
    EXTRACT(YEAR FROM Date) AS Year,
    EXTRACT(DOW FROM Date) AS DayOfWeek,
    CASE WHEN EXTRACT(DOW FROM Date) IN (0, 6) THEN TRUE ELSE FALSE END AS IsWeekend
    FROM
    (   SELECT 
        DATEADD(DAY, SEQ4(), '1900-01-01') AS Date
        FROM TABLE(GENERATOR(ROWCOUNT => 10000000))) 
    ) a where date<= '9999-12-31'