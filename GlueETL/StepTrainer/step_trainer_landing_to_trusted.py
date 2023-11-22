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

# Script generated for node Customer Curated
CustomerCurated_node1700489415968 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1700489415968",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1700489414375 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1700489414375",
)

# Script generated for node Join Tables, Drop Fields and Duplicates
SqlQuery469 = """
select distinct trainer_landing.serialnumber, sensorreadingtime, distancefromobject from trainer_landing join customer_curated on trainer_landing.serialnumber=customer_curated.serialnumber
"""
JoinTablesDropFieldsandDuplicates_node1700489426266 = sparkSqlQuery(
    glueContext,
    query=SqlQuery469,
    mapping={
        "trainer_landing": StepTrainerLanding_node1700489414375,
        "customer_curated": CustomerCurated_node1700489415968,
    },
    transformation_ctx="JoinTablesDropFieldsandDuplicates_node1700489426266",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1700489430254 = glueContext.getSink(
    path="s3://bastis-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1700489430254",
)
StepTrainerTrusted_node1700489430254.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1700489430254.setFormat("json")
StepTrainerTrusted_node1700489430254.writeFrame(
    JoinTablesDropFieldsandDuplicates_node1700489426266
)
job.commit()
