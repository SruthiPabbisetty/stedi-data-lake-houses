import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1783429321481 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-houses/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1783429321481")

# Script generated for node accelerometer_landing
accelerometer_landing_node1783429249652 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-houses/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1783429249652")

# Script generated for node Join
Join_node1783429303741 = Join.apply(frame1=accelerometer_landing_node1783429249652, frame2=customer_trusted_node1783429321481, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1783429303741")

# Script generated for node Change Schema
ChangeSchema_node1783429388385 = ApplyMapping.apply(frame=Join_node1783429303741, mappings=[("z", "double", "z", "double"), ("user", "string", "user", "string"), ("y", "double", "y", "double"), ("x", "double", "x", "double"), ("timestamp", "long", "timestamp", "long"), ("lastUpdateDate", "long", "lastUpdateDate", "long"), ("phone", "string", "phone", "string")], transformation_ctx="ChangeSchema_node1783429388385")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1783429388385, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783429146605", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1783429426161 = glueContext.getSink(path="s3://stedi-data-lake-houses/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1783429426161")
accelerometer_trusted_node1783429426161.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1783429426161.setFormat("json")
accelerometer_trusted_node1783429426161.writeFrame(ChangeSchema_node1783429388385)
job.commit()