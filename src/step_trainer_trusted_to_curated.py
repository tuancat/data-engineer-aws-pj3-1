import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted Glue Data Catalog
step_trainer_trustedGlueDataCatalog_node1699746437870 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi-lakehouse",
        table_name="step_trainer_trusted",
        transformation_ctx="step_trainer_trustedGlueDataCatalog_node1699746437870",
    )
)



# Script generated for node accelerometer_trusted Glue Data Catalog
accelerometer_trustedGlueDataCatalog_node1699746486319 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi-lakehouse",
        table_name="accelerometer_trusted",
        transformation_ctx="accelerometer_trustedGlueDataCatalog_node1699746486319",
    )
)

# Script generated for node Join
Join_node1699746530555 = Join.apply(
    frame1=accelerometer_trustedGlueDataCatalog_node1699746486319,
    frame2=step_trainer_trustedGlueDataCatalog_node1699746437870,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1699746530555",
)

# Script generated for node Drop Fields
DropFields_node1679503544811 = DropFields.apply(
    frame=Join_node1699746530555,
    paths=["user"],
    transformation_ctx="DropFields_node1679503544811",
)

# Script generated for node Amazon S3
AmazonS3_node1699746769918 = glueContext.getSink(
    path="s3://stedi-lake-house-tuancat/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1699746769918",
)
AmazonS3_node1699746769918.setCatalogInfo(
    catalogDatabase="stedi-lakehouse", catalogTableName="machine_learning_curated"
)
AmazonS3_node1699746769918.setFormat("json")
AmazonS3_node1699746769918.writeFrame(DropFields_node1679503544811)
job.commit()
