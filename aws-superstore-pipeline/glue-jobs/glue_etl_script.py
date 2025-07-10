import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize contexts and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 paths
input_s3_path = "s3://your-input-bucket/raw-data/"
output_s3_path = "s3://your-output-bucket/parquet-data/"

# Read CSV from S3
df = spark.read.option("header", True).csv(input_s3_path)

# Convert and write to Parquet in S3
df.write.mode("overwrite").parquet(output_s3_path)

job.commit()
