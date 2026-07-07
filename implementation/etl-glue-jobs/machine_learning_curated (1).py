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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1783430988093 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-houses/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1783430988093")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1783430891869 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-houses/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1783430891869")

# Script generated for node SQL Query
SqlQuery3256 = '''
select a.user,a.x,a.y,a.z,s.*
from a
join s
on a.timestamp=s.sensorreadingtime
'''
SQLQuery_node1783431020399 = sparkSqlQuery(glueContext, query = SqlQuery3256, mapping = {"s":step_trainer_trusted_node1783430988093, "a":accelerometer_trusted_node1783430891869}, transformation_ctx = "SQLQuery_node1783431020399")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783431020399, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783430876773", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1783431132540 = glueContext.getSink(path="s3://stedi-data-lake-houses/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1783431132540")
machine_learning_curated_node1783431132540.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1783431132540.setFormat("json")
machine_learning_curated_node1783431132540.writeFrame(SQLQuery_node1783431020399)
job.commit()