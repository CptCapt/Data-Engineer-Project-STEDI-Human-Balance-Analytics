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

# Script generated for node Customer Trusted
CustomerTrusted_node1700409358627 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1700409358627",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1700409353538 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1700409353538",
)

# Script generated for node Join
SqlQuery420 = """
select * from accelerometer_landing join customer_trusted on accelerometer_landing.user=customer_trusted.email

"""
Join_node1700409396930 = sparkSqlQuery(
    glueContext,
    query=SqlQuery420,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1700409353538,
        "customer_trusted": CustomerTrusted_node1700409358627,
    },
    transformation_ctx="Join_node1700409396930",
)

# Script generated for node Drop Fields
SqlQuery419 = """
select user, timestamp, x, y, z from myDataSource

"""
DropFields_node1700410374879 = sparkSqlQuery(
    glueContext,
    query=SqlQuery419,
    mapping={"myDataSource": Join_node1700409396930},
    transformation_ctx="DropFields_node1700410374879",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1700412186696 = glueContext.getSink(
    path="s3://bastis-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1700412186696",
)
AccelerometerTrusted_node1700412186696.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1700412186696.setFormat("json")
AccelerometerTrusted_node1700412186696.writeFrame(DropFields_node1700410374879)
job.commit()
