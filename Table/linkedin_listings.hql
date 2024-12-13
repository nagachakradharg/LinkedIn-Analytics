-- create csv tables

CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_job_postings_csv
(
    job_link            STRING,
    last_processed_time STRING,
    got_summary         STRING,
    got_ner             STRING,
    is_being_worked     STRING,
    job_title           STRING,
    company             STRING,
    job_location        STRING,
    first_seen          STRING,
    search_city         STRING,
    search_country      STRING,
    search_position     STRING,
    job_level           STRING,
    job_type            STRING
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar" = "\"")
    STORED AS TEXTFILE
    LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/nagachakradharg/linkedin_postings'
    TBLPROPERTIES ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_job_skills_csv
(
    job_link   STRING,
    job_skills STRING
)
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = ",", 
        "quoteChar" = "\"")
    STORED AS TEXTFILE
    LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/nagachakradharg/linkedin_skills'
    TBLPROPERTIES ("skip.header.line.count"="1");


-- create ORC tables

CREATE TABLE IF NOT EXISTS nagachakradharg_linkedin_job_postings
(
    job_link            STRING,
    last_processed_time STRING,
    got_summary         STRING,
    got_ner             STRING,
    is_being_worked     STRING,
    job_title           STRING,
    company             STRING,
    job_location        STRING,
    first_seen          STRING,
    search_city         STRING,
    search_country      STRING,
    search_position     STRING,
    job_level           STRING,
    job_type            STRING
)
    STORED AS ORC;

insert overwrite table nagachakradharg_linkedin_job_postings select * from nagachakradharg_linkedin_job_postings_csv;


CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_job_skills
(
    job_link   STRING,
    job_skills STRING
)
    STORED AS ORC;

insert overwrite table nagachakradharg_linkedin_job_skills select * from nagachakradharg_linkedin_job_skills_csv;

CREATE EXTERNAL TABLE nagachakradharg_linkedin_exploded_skills AS
SELECT
    job_link,
    skill
FROM
    nagachakradharg_linkedin_job_skills
        LATERAL VIEW explode(split(job_skills, ',')) exploded AS skill
WHERE
    job_skills IS NOT NULL;

-- Location Stats

-- Create table with explicit column families and ensure non-null key
CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_location_heat_map_hbase (
    location_key STRING,
    job_count STRING,
    avg_duration_days STRING,
    remote_job_count STRING,
    unique_companies STRING,
    top_job_type STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
    ':key,stats:job_count,stats:avg_duration_days,stats:remote_job_count,stats:unique_companies,stats:top_job_type')
TBLPROPERTIES ('hbase.table.name' = 'nagachakradharg_linkedin_location_heat_map_hbase');

-- Insert with NULL handling
INSERT OVERWRITE TABLE nagachakradharg_linkedin_location_heat_map_hbase
SELECT 
    COALESCE(job_location, 'UNKNOWN') as location_key,
    CAST(COUNT(*) AS STRING) as job_count,
    CAST(AVG(DATEDIFF(
            TO_DATE(last_processed_time),
            TO_DATE(first_seen)
             )) AS FLOAT) AS avg_duration_days,
    CAST(SUM(CASE 
        WHEN LOWER(job_location) LIKE '%remote%' 
        OR LOWER(job_type) LIKE '%remote%' 
        THEN 1 ELSE 0 
    END) AS STRING) as remote_job_count,
    CAST(COUNT(DISTINCT company) AS STRING) as unique_companies,
    COALESCE(MAX(job_type), 'UNKNOWN') as top_job_type
FROM 
    nagachakradharg_linkedin_job_postings
WHERE 
    job_location IS NOT NULL
    AND TRIM(job_location) != ''  -- Ensure non-empty strings
GROUP BY 
    job_location;



--Features that can  be included once we have more varied data


CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_countrywide_features_hbase
(
    search_country      STRING,
    open_count BIGINT,
    total_count BIGINT,
    avg_duration_days FLOAT
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES ('hbase.columns.mapping' =
            ':key,country:open_count#b,country:total_count#b,country:avg_duration_days#b')
    TBLPROPERTIES ('hbase.table.name' = 'nagachakradharg_linkedin_countrywide_features_hbase');

INSERT OVERWRITE TABLE nagachakradharg_linkedin_countrywide_features_hbase
SELECT
    search_country,
    SUM(CASE WHEN is_being_worked = 't' THEN 1 ELSE 0 END) AS open_count,
    COUNT(*) AS total_count,
    CAST(AVG(DATEDIFF(
            TO_DATE(last_processed_time),
            TO_DATE(first_seen)
             )) AS FLOAT) AS avg_duration_days
FROM nagachakradharg_linkedin_job_postings
WHERE search_country IS NOT NULL
GROUP BY search_country;


CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_citywide_features_hbase
(
    search_city     STRING,
    open_count BIGINT,
    total_count BIGINT,
    avg_duration_days FLOAT
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES ('hbase.columns.mapping' =
            ':key,city:open_count#b,city:total_count#b,city:avg_duration_days#b')
    TBLPROPERTIES ('hbase.table.name' = 'nagachakradharg_linkedin_citywide_features_hbase');

INSERT OVERWRITE TABLE nagachakradharg_linkedin_citywide_features_hbase
SELECT
    search_city,
    SUM(CASE WHEN is_being_worked = 't' THEN 1 ELSE 0 END) AS open_count,
    COUNT(*) AS total_count,
    CAST(AVG(DATEDIFF(
            TO_DATE(last_processed_time),
            TO_DATE(first_seen)
             )) AS FLOAT) AS avg_duration_days
FROM nagachakradharg_linkedin_job_postings
WHERE search_city IS NOT NULL
GROUP BY search_city;

-- useful with larger dataset
CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_job_year_features_hbase (
    year                STRING,
    avg_duration_jan    FLOAT,
    avg_duration_feb    FLOAT,
    avg_duration_mar    FLOAT,
    avg_duration_apr    FLOAT,
    avg_duration_may    FLOAT,
    avg_duration_jun    FLOAT,
    avg_duration_jul    FLOAT,
    avg_duration_aug    FLOAT,
    avg_duration_sep    FLOAT,
    avg_duration_oct    FLOAT,
    avg_duration_nov    FLOAT,
    avg_duration_dec    FLOAT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
':key,months:jan#b,months:feb#b,months:mar#b,months:apr#b,months:may#b,months:jun#b,months:jul#b,months:aug#b,months:sep#b,months:oct#b,months:nov#b,months:dec#b')
TBLPROPERTIES ('hbase.table.name' = 'nagachakradharg_linkedin_job_year_features_hbase');


INSERT INTO TABLE nagachakradharg_linkedin_job_year_features_hbase
SELECT
    YEAR(TO_DATE(first_seen)) AS year,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 1 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_jan,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 2 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_feb,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 3 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_mar,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 4 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_apr,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 5 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_may,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 6 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_jun,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 7 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_jul,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 8 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_aug,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 9 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_sep,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 10 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_oct,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 11 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_nov,
    CAST(AVG(CASE WHEN MONTH(TO_DATE(first_seen)) = 12 THEN DATEDIFF(TO_DATE(last_processed_time), TO_DATE(first_seen)) ELSE NULL END) AS FLOAT) AS avg_duration_dec
FROM
    nagachakradharg_linkedin_job_postings
WHERE first_seen IS NOT NULL
GROUP BY
    YEAR(TO_DATE(first_seen));


CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_top_companies_by_level
(
    job_level STRING,
    company STRING,
    total_job_postings BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,details:company,details:total_job_postings#b')
TBLPROPERTIES ('hbase.table.name' = 'nagachakradharg_top_companies_by_level');



INSERT OVERWRITE TABLE nagachakradharg_top_companies_by_level
SELECT 
    job_level,
    company,
    COUNT(*) AS total_job_postings
FROM 
    nagachakradharg_linkedin_job_postings
WHERE job_level is not null AND company IS NOT NULL
GROUP BY 
    job_level, 
    company
ORDER BY 
    job_level, 
    total_job_postings DESC;

-- query job levels as such
SELECT 
    job_level, 
    company, 
    total_job_postings
FROM 
    nagachakradharg_top_companies_by_level
DISTRIBUTE BY job_level
SORT BY total_job_postings DESC
LIMIT 5;

---
CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_top_skills_level_hbase
(
    table_id STRING,
    job_level    STRING,
    skill        STRING,
    skill_count  BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' =
    ':key,skills:job_level#b,skills:skill#b,skills:skill_count#b')
TBLPROPERTIES ('hbase.table.name' = 'nagachakradharg_linkedin_top_skills_level_hbase');



-- shou;d be able to query as such
SELECT job_level, skill, skill_count
FROM nagachakradharg_linkedin_top_skills_level_hbase
WHERE job_level = 'Entry level'
ORDER BY skill_count DESC
LIMIT 5;

CREATE EXTERNAL TABLE nagachakradharg_linkedin_exploded_skills AS
SELECT
    job_link,
    skill
FROM
    nagachakradharg_linkedin_job_skills
        LATERAL VIEW explode(split(job_skills, ',')) exploded AS skill
WHERE
    job_skills IS NOT NULL;


INSERT OVERWRITE TABLE nagachakradharg_linkedin_top_skills_level_hbase
SELECT
   CONCAT(postings.job_level, '_', exploded_skills.skill) as table_id,
    postings.job_level,
    exploded_skills.skill,
    COUNT(*) AS skill_count
FROM
    nagachakradharg_linkedin_job_postings postings
        JOIN
    nagachakradharg_linkedin_exploded_skills exploded_skills
    ON postings.job_link = exploded_skills.job_link
WHERE
    postings.job_level IS NOT NULL
GROUP BY
    postings.job_level, exploded_skills.skill
ORDER BY
    postings.job_level, skill_count DESC;


INSERT OVERWRITE TABLE nagachakradharg_linkedin_top_skills_level_hbase
SELECT 
    table_id,
    job_level,
    skill,
    skill_count
FROM (
    SELECT 
        CONCAT(postings.job_level, '_', exploded_skills.skill) as table_id,
        postings.job_level,
        exploded_skills.skill,
        COUNT(*) AS skill_count,
        ROW_NUMBER() OVER (PARTITION BY postings.job_level ORDER BY COUNT(*) DESC) as skill_rank
    FROM
        nagachakradharg_linkedin_job_postings postings
            JOIN
        nagachakradharg_linkedin_exploded_skills exploded_skills
        ON postings.job_link = exploded_skills.job_link
    WHERE
        postings.job_level IS NOT NULL
    GROUP BY
        postings.job_level, exploded_skills.skill
) ranked_skills
WHERE skill_rank <= 5
ORDER BY job_level, skill_count DESC;