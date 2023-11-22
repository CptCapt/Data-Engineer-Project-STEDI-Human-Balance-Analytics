import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing_zone
customer_landing_zone_node1699459580706 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customer_landing_zone_node1699459580706",
)

# Script generated for node SQL Query Filter
SqlQuery33 = """
select * from myDataSource where sharewithresearchasofdate <> 0;
"""
SQLQueryFilter_node1699373097637 = sparkSqlQuery(
    glueContext,
    query=SqlQuery33,
    mapping={"myDataSource": customer_landing_zone_node1699459580706},
    transformation_ctx="SQLQueryFilter_node1699373097637",
)

# Script generated for node trusted_customer_zone
trusted_customer_zone_node1699098386375 = glueContext.getSink(
    path="s3://bastis-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="trusted_customer_zone_node1699098386375",
)
trusted_customer_zone_node1699098386375.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
trusted_customer_zone_node1699098386375.setFormat("json")
trusted_customer_zone_node1699098386375.writeFrame(SQLQueryFilter_node1699373097637)
job.commit()
