SELECT count(*) as "customer_landing_count" FROM "stedi-lakehouse"."customer_landinglanding";

SELECT count(*) as "accelerometer_landing_count" FROM "stedi-lakehouse"."accelerometer_landinglanding";

SELECT count(*) as "customer_trusted_count" FROM "stedi-lakehouse"."customer_trusted";
SELECT count(*) as "accelerometer_trusted_count" FROM "stedi-lakehouse"."accelerometer_trusted";
SELECT count(distinct serialnumber) as "step_trainer_trusted_count" FROM "stedi-lakehouse"."step_trainer_trusted";
SELECT count(distinct email) as "customer_curated_count" FROM "stedi-lakehouse"."customer_curated";