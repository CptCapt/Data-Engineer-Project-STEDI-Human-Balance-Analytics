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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1700484566416 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1700484566416",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1700484564628 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1700484564628",
)

# Script generated for node Join
SqlQuery727 = """
select * from customer_trusted join accelerometer_landing where customer_trusted.email=accelerometer_landing.user

"""
Join_node1700484652335 = sparkSqlQuery(
    glueContext,
    query=SqlQuery727,
    mapping={
        "customer_trusted": CustomerTrusted_node1700484564628,
        "accelerometer_landing": AccelerometerLanding_node1700484566416,
    },
    transformation_ctx="Join_node1700484652335",
)

# Script generated for node Drop Fields and Duplicates
SqlQuery728 = """
select distinct customername, email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate from myDataSource

"""
DropFieldsandDuplicates_node1700484907876 = sparkSqlQuery(
    glueContext,
    query=SqlQuery728,
    mapping={"myDataSource": Join_node1700484652335},
    transformation_ctx="DropFieldsandDuplicates_node1700484907876",
)

# Script generated for node Customer Curated
CustomerCurated_node1700485450986 = glueContext.getSink(
    path="s3://bastis-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1700485450986",
)
CustomerCurated_node1700485450986.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1700485450986.setFormat("json")
CustomerCurated_node1700485450986.writeFrame(DropFieldsandDuplicates_node1700484907876)
job.commit()
