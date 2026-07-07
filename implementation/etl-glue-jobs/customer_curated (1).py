import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node customer_customer_trusted
customer_customer_trusted_node1783429872757 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-houses/customer/trusted/"], "recurse": True}, transformation_ctx="customer_customer_trusted_node1783429872757")

# Script generated for node acceleromete_landing
acceleromete_landing_node1783429811127 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-houses/accelerometer/landing/"], "recurse": True}, transformation_ctx="acceleromete_landing_node1783429811127")

# Script generated for node SQL Query
SqlQuery357 = '''
select distinct c.*
from a
join c 
on a.user=c.email

'''
SQLQuery_node1783429949007 = sparkSqlQuery(glueContext, query = SqlQuery357, mapping = {"a":acceleromete_landing_node1783429811127, "c":customer_customer_trusted_node1783429872757}, transformation_ctx = "SQLQuery_node1783429949007")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783429949007, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783429722818", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1783430081566 = glueContext.getSink(path="s3://stedi-data-lake-houses/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1783430081566")
customer_curated_node1783430081566.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1783430081566.setFormat("json")
customer_curated_node1783430081566.writeFrame(SQLQuery_node1783429949007)
job.commit()