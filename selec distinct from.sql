CREATE OR REPLACE VIEW PPA1_vw AS
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
    h."GENERATION" * c.price AS "Notional",
    pr."project"
FROM 
    ETRM_PUBLIC.CHILDDEAL c
JOIN ETRM_PUBLIC.PARENTDEAL p ON p."id" = c."parentdeal_id" 
JOIN ETRM_PUBLIC.CHILDDEAL c
JOIN ETRM_PUBLIC.COUNTERPARTY  c2 ON c2."id" = p."counterparty_id" 
JOIN ETRM_PUBLIC.HOURLYPROFILE h ON c."profile_id" = h."PROFILE_ID"
JOIN ETRM_PUBLIC.PRODUCT p2 ON c."product_id" = p2."id"
right join ETRM_PUBLIC.CHILDDEAL_PROJECTS cpr on c."id" = cpr."childdeal_id"
RIGHT JOIN ETRM_PUBLIC.PROJECTS pr ON cpr."project_id" = pr."id" -- This is the right join with the Project table
WHERE 
    c."parentdeal_id" IS NOT NULL
    AND c."profile_id" IS NOT NULL
    AND p.end_date >= DATE_TRUNC('month', NOW())::DATE
    AND c.end_date >= DATE_TRUNC('month', NOW())::DATE
    AND h."DATEX" >= c.start_date
    AND h."DATEX" <= c.end_date
    AND h."DATEX" >= DATE_TRUNC('month', NOW())::DATE
    AND h."PROFILE_ID" NOT IN (1,2,37)
    AND TRIM(p2.product) NOT IN ('Energy','Storage')
    AND TRIM(p2.product_type) <> 'RA'
 ;