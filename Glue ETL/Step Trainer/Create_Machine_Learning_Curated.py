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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1700657693964 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1700657693964",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1700657691579 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1700657691579",
)

# Script generated for node Join, Drop Fields
SqlQuery444 = """
select sensorreadingtime, serialnumber, user, distancefromobject, x, y, z from trainer_trusted join accelerometer_trusted on trainer_trusted.sensorreadingtime=accelerometer_trusted.timestamp

"""
JoinDropFields_node1700657696650 = sparkSqlQuery(
    glueContext,
    query=SqlQuery444,
    mapping={
        "trainer_trusted": StepTrainerTrusted_node1700657693964,
        "accelerometer_trusted": AccelerometerTrusted_node1700657691579,
    },
    transformation_ctx="JoinDropFields_node1700657696650",
)

# Script generated for node Amazon S3
AmazonS3_node1700657699132 = glueContext.getSink(
    path="s3://bastis-lake-house/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1700657699132",
)
AmazonS3_node1700657699132.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1700657699132.setFormat("json")
AmazonS3_node1700657699132.writeFrame(JoinDropFields_node1700657696650)
job.commit()
