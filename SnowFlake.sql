select distinct "FILE_DATE" from  ETRM_PUBLIC.HOURLYPROFILE

create or replace temporary table IST1 as 
select p."id" as "PARENT ID",p."contract_exec_date" as "Trade Date",
c2."counterparty_name", 
c."contract_number", 
p2."product",
p2."product_type",
p2."productsubtype",
p."start_date" as "P_START", 
p."end_date" as "P_END", 
c."start_date" as "C_START",
c."end_date" as "C_END",
c."id" as "CHILD_ID", 
c."profile_id",
c."direction", 
c."volume",
c."volume_unit",
c."price",
c."price_unit",
h."DATEX" ,
h."HE",
h."GENERATION",
h."GENERATION" * c."volume" as "Supply",
h."GENERATION" * c."volume" * c."price" as "Notional"
from ETRM_PUBLIC.PARENTDEAL p, ETRM_PUBLIC.CHILDDEAL c, ETRM_PUBLIC.COUNTERPARTY c2, ETRM_PUBLIC.HOURLYPROFILE h,  ETRM_PUBLIC.PRODUCT p2
where p."id"=c."parentdeal_id" 
and c."parentdeal_id" is not null
and c2."id" = p."counterparty_id" 
and c."profile_id" = h."PROFILE_ID" 
and c."profile_id" is not null
and c."end_date" >= '2024-04-01'
and h."DATEX">= c."start_date"
and h."DATEX"<= c."end_date"
and h."DATEX">= '2024-04-01'
and c."product_id" = p2."id"
and h."PROFILE_ID" in (1,2,37)
and trim(p2."product") not in ('Storage')
and trim(p2."product_type") <> 'RA'
limit 100
;
--) IST1
--group by "DATEX", "HE"

/* IST3 will be combined with other hourly tables
 
 */

create or replace temporary table IST3 as
SELECT "DATEX" ,
"HE",
sum("Supply") as HourlyISTMWh,
sum("Notional") as HourlyISTNotional
from IST1
group by "DATEX", "HE"
;


CREATE or replace TEMPORARY TABLE PPA1 AS
SELECT 
    p."id" AS "PARENT ID",
    p."contract_exec_date" AS "Trade Date",
    c2."counterparty_name", 
    c."contract_number",
    p2."product",
    p2."product_type",
    p2."productsubtype",
    p."start_date" AS "P_START", 
    p."end_date" AS "P_END", 
    c."start_date" AS "C_START",
    c."end_date" AS "C_END",
    c."id" AS "CHILD_ID", 
    c."profile_id",
    c."direction", 
    c."volume",
    c."volume_unit",
    c."price",
    c."price_unit",
    h."DATEX",
    h."HE",
    h."GENERATION" AS "Supply",
    h."GENERATION" * c."price" AS "Notional",
    pr."project"
FROM   ETRM_PUBLIC.CHILDDEAL  c 
JOIN ETRM_PUBLIC.PARENTDEAL p ON p."id" = c."parentdeal_id" 
JOIN ETRM_PUBLIC.COUNTERPARTY  c2 ON c2."id" = p."counterparty_id" 
JOIN ETRM_PUBLIC.HOURLYPROFILE h  ON c."profile_id" = h."PROFILE_ID"
JOIN ETRM_PUBLIC.PRODUCT p2 ON c."product_id" = p2."id"
right join ETRM_PUBLIC.childdeal_projects cpr on c."id" = cpr."childdeal_id"
RIGHT JOIN ETRM_PUBLIC.projects pr ON cpr."project_id" = pr."id" -- This is the right join with the Project table
WHERE 
    c."parentdeal_id" IS NOT NULL
    AND c."profile_id" IS NOT NULL
    AND p."end_date" >= DATE_TRUNC('month', CURRENT_TIMESTAMP())::DATE
    AND c."end_date" >= DATE_TRUNC('month', CURRENT_TIMESTAMP())::DATE
    AND h."DATEX" >= c."start_date"
    AND h."DATEX" <= c."end_date"
    AND h."DATEX" >= DATE_TRUNC('month', CURRENT_TIMESTAMP())::DATE
    AND h."PROFILE_ID" NOT IN (1,2,37)
    AND TRIM(p2."product") NOT IN ('Energy','Storage')
    AND TRIM(p2."product_type") <> 'RA' ;


create or replace temporary table PPA3 as
select"DATEX" ,
"HE",
sum("Supply") as HourlyPPAMWh,
sum("Notional") as HourlyPPANotional
from PPA1
group by "DATEX", "HE"
;


CREATE or replace TEMPORARY TABLE Storage1 AS
SELECT 
    p."id" AS "PARENT ID",
    p."contract_exec_date" AS "Trade Date",
    c2."counterparty_name", 
    c."contract_number",
    p2."product",
    p2."product_type",
    p2."productsubtype",
    p."start_date" AS "P_START", 
    p."end_date" AS "P_END", 
    c."start_date" AS "C_START",
    c."end_date" AS "C_END",
    c."id" AS "CHILD_ID", 
    c."profile_id",
    c."direction", 
    c."volume",
    c."volume_unit",
    c."price",
    c."price_unit",
    h."DATEX",
    h."HE",
    h."GENERATION" AS "Supply",
    CASE 
        WHEN EXTRACT(DAY FROM h."DATEX") = 1 AND h."HE" = 1 THEN c."volume" * c."price" * 1000
        ELSE 0 
    END AS "Notional",
    pr."project"
FROM 
     ETRM_PUBLIC.CHILDDEAL c
JOIN ETRM_PUBLIC.parentdeal p ON p."id" = c."parentdeal_id"
JOIN ETRM_PUBLIC.counterparty c2 ON c2."id" = p."counterparty_id" 
JOIN ETRM_PUBLIC.hourlyprofile h ON c."profile_id" = h."PROFILE_ID"
JOIN ETRM_PUBLIC.product p2 ON c."product_id" = p2."id"
RIGHT JOIN ETRM_PUBLIC.childdeal_projects cpr on c."id" = cpr."childdeal_id"
RIGHT JOIN ETRM_PUBLIC.projects pr ON cpr."project_id" = pr."id" -- This is the right join with the Project table
WHERE 
    c."parentdeal_id" IS NOT NULL
    AND c."profile_id" IS NOT NULL
    AND p."end_date" >= DATE_TRUNC('month', CURRENT_TIMESTAMP())::DATE
    AND c."end_date" >= DATE_TRUNC('month', CURRENT_TIMESTAMP())::DATE
    AND h."DATEX" >= c."start_date"
    AND h."DATEX" <= c."end_date"
    AND h."DATEX" >= DATE_TRUNC('month', CURRENT_TIMESTAMP())::DATE
    AND h."PROFILE_ID" NOT IN (1,2,37)
    AND TRIM(p2."product") IN ('Storage')
    AND TRIM(p2."product_type") <> 'RA'
    ;

create or replace temporary table Storage3 as
select"DATEX" ,
"HE",
sum("Supply") as HourlyStorageMWh,
sum("Notional") as HourlyStorageNotional
from Storage1
group by "DATEX", "HE"
;


create or replace temporary table ScenarioFimoCostHourlySummary1 as
select l."datex" as "DATE",
l."hour_ending" as he,
extract(year from l."datex") as "year",
extract(month from l."datex") as "month",
l."retail_mwh" as "RETAIL_MWH",
l."wholesale_mwh" as "WHOLESALE_MWH",
l."version_desc",
ist.HourlyISTMWh,
ist.HourlyISTNotional
from IST3 ist, ETRM_PUBLIC.loadx as l
where l."datex" = ist."DATEX"
and l."hour_ending" = ist."HE"
--and l.version_desc='2024 April'
;



create or replace temporary table ScenarioFimoCostHourlySummary2 as
select 
sfc."DATE",
sfc.he,
sfc."year",
sfc."month",
sfc."RETAIL_MWH",
sfc."WHOLESALE_MWH",
sfc.HourlyISTMWh,
sfc.HourlyISTNotional,
sfc."version_desc",
p.HourlyPPAMWh,
p.HourlyPPANotional
from PPA3 p, ScenarioFimoCostHourlySummary1 sfc
where sfc."DATE" = p."DATEX"
and sfc.he = p."HE"
;



create or replace temporary table ScenarioFimoCostHourlySummary3 as
select sfc."DATE",
sfc.he,
sfc."year",
sfc."month",
sfc."RETAIL_MWH",
sfc."WHOLESALE_MWH"  ,
sfc.HourlyISTMWh as "ist",
sfc.HourlyISTNotional as "HourlyISTNotional",
sfc.HourlyPPAMWh as "ppa",
sfc.HourlyPPANotional as "HourlyPPANotional",
sfc."version_desc",
s.HourlyStorageMWh as "storage" ,
s.HourlyStorageNotional as "HourlyStorageNotional",
sfc."WHOLESALE_MWH" - hourlyistmwh - hourlyppamwh - hourlystoragemwh as "HourlyDeficit"
from Storage3 s, ScenarioFimoCostHourlySummary2 sfc
where sfc."DATE" = s."DATEX"
and sfc.he = s."HE"
order by 
sfc."DATE",
sfc.he;



select "DATE", sfc.he as "he", "year", "month", "version_desc" as "version_desc" , cal."peak_west"
, MW_Type as "mw_type", MW as "mw", Demand_Type as "demand_type",  Demand_MW as "demand_mw"
, current_timestamp as "Report Timestamp"
from ScenarioFimoCostHourlySummary3 as sfc
unpivot(MW 
for MW_Type in ("ist", "ppa", "storage"))
unpivot( Demand_MW 
for Demand_Type in ("RETAIL_MWH", "WHOLESALE_MWH", "HourlyDeficit"))
left join ETRM_FIMO._calendar_s as cal
on sfc."DATE" = cal."date_"
and sfc.he=cal."hour_"
