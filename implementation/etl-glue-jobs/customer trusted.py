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

# Script generated for node customer landing
customerlanding_node1783425828503 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-houses/customer/landing/"], "recurse": True}, transformation_ctx="customerlanding_node1783425828503")

# Script generated for node SQL Query
SqlQuery3193 = '''
select * from myDataSource
where shareWithResearchAsofDate is not null
'''
SQLQuery_node1783426111847 = sparkSqlQuery(glueContext, query = SqlQuery3193, mapping = {"myDataSource":customerlanding_node1783425828503}, transformation_ctx = "SQLQuery_node1783426111847")

# Script generated for node customer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783426111847, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783426062884", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customertrusted_node1783426175219 = glueContext.getSink(path="s3://stedi-data-lake-houses/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customertrusted_node1783426175219")
customertrusted_node1783426175219.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customertrusted_node1783426175219.setFormat("json")
customertrusted_node1783426175219.writeFrame(SQLQuery_node1783426111847)
job.commit()