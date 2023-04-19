import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "de_youtube_raw", table_name = "raw_statistics", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "de_youtube_raw", table_name = "raw_statistics", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("video_id", "string", "video_id", "string"), ("title", "string", "title", "string"), ("publishedat", "string", "publishedat", "string"), ("channelid", "string", "channelid", "long"), ("channeltitle", "string", "channeltitle", "string"), ("categoryid", "string", "categoryid", "string"), ("trending_date", "string", "trending_date", "string"), ("tags", "string", "tags", "string"), ("view_count", "string", "view_count", "long"), ("likes", "string", "likes", "long"), ("dislikes", "string", "dislikes", "long"), ("comment_count", "string", "comment_count", "long"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "string", "comments_disabled", "string"), ("ratings_disabled", "string", "ratings_disabled", "string"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("video_id", "string", "video_id", "string"), ("title", "string", "title", "string"), ("publishedat", "string", "publishedat", "string"), ("channelid", "string", "channelid", "long"), ("channeltitle", "string", "channeltitle", "string"), ("categoryid", "string", "categoryid", "string"), ("trending_date", "string", "trending_date", "string"), ("tags", "string", "tags", "string"), ("view_count", "string", "view_count", "long"), ("likes", "string", "likes", "long"), ("dislikes", "string", "dislikes", "long"), ("comment_count", "string", "comment_count", "long"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "string", "comments_disabled", "string"), ("ratings_disabled", "string", "ratings_disabled", "string"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://dataengineer-youtube-cleansed-apsoutheast1-dev/youtube/raw_statistics/"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]

datasink1 = dropnullfields3.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output") 

datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://dataengineer-youtube-cleansed-apsoutheast1-dev/youtube/raw_statistics/", "partitionKeys": ["region"]}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
