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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681917802950 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1681917802950",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681917765232 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="cleaned_statistics_referennce_data",
    transformation_ctx="AWSGlueDataCatalog_node1681917765232",
)

# Script generated for node Join
Join_node1681917833959 = Join.apply(
    frame1=AWSGlueDataCatalog_node1681917802950,
    frame2=AWSGlueDataCatalog_node1681917765232,
    keys1=["categoryid"],
    keys2=["id"],
    transformation_ctx="Join_node1681917833959",
)

# Script generated for node Amazon S3
AmazonS3_node1681918055467 = glueContext.getSink(
    path="s3://dataengineer-youtube-analytics-apsoutheast1-dev",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "categoryid"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681918055467",
)
AmazonS3_node1681918055467.setCatalogInfo(
    catalogDatabase="db_youtube_analytics", catalogTableName="final_analytics"
)
AmazonS3_node1681918055467.setFormat("glueparquet")
AmazonS3_node1681918055467.writeFrame(Join_node1681917833959)
job.commit()

