# Project: STEDI-Human-Balance-Analytics

---
## Project Overview

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
- Trains the user to do a STEDI balance exercise
- Has sensors on the device that collect data to train a machine-learning algorithm to detect steps
- Has a companion mobile app that collects customer data and interacts with the device sensors

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

---

### Problem Discription

In this project I extracted data produced by the STEDI Step Trainer sensors and the mobile app, and curated them into a data lakehouse solution on AWS. The intent is for Data Scientists to use the solution to train machine learning models. 

The Data lake solution is developed using AWS Glue, AWS S3, Python, and Spark for sensor data that trains machine learning algorithms.

AWS infrastructure is used to create storage zones (landing, trusted and curated), data catalog, data transformations between zones and queries in semi-structured data.

---

## Project Datasets
The provided datasets for customer, accelerometer and step trainer are JSON formatted data from different sources.
* Customer Records: Fulfillment and the STEDI website.  
* Step Trainer Records: From the motion sensor.
* Accelerometer Records: From the mobile app.

---

## Implementation Summary

**NOTE: The following images show the AWS setup in german language.**

<details>
<summary>
Landing Zone
</summary>

In the Landing Zone the raw data from customer, accelerometer and step trainer are each stored in S3 buckets. 
By using AWS Athena I created corresponding Glue tables for the Data Catalog and further data transformation.

The following images show relevant Athena Queries to verify the correct table creation and the correct amount of data points.

**1- Customer Landing Table:**

![alt text](AthenaQueries/Screenshots/customer_landing.png)

**2- Accelerometer Landing Table:**

![alt text](AthenaQueries/Screenshots/accelerometer_landing.png)

**3- Step Trainer Landing Table:**

![alt text](AthenaQueries/Screenshots/step_trainer_landing.png)

</details>

<details>
<summary>
Trusted Zone
</summary>

In the Trusted Zone, I created AWS Glue jobs to transform the raw data from the landing zones to the corresponding trusted zones. In conclusion, it only contains customer records from people who agreed to share their data.

**Glue job scripts**

[1. customer_landing_to_trusted.py](GlueETL/Customer/customer_landing_to_trusted.py) - This script transfers customer data from the 'landing' to 'trusted' zones. It filters for customers who have agreed to share data with researchers.

[2. accelerometer_landing_to_trusted.py](GlueETL/accelerometer/accelerometer_landing_to_trusted.py) - This script transfers accelerometer data from the 'landing' to 'trusted' zones. Using a join on customer_trusted and accelerometer_landing, It filters for Accelerometer readings from customers who have agreed to share data with researchers.

[3. step_trainer_landing_to_trusted.py](GlueETL/StepTrainer/step_trainer_landing_to_trusted.py) - This script transfers Step Trainer data from the 'landing' to 'trusted' zones. Using a join on customer_curated and step_trainer_landing, It filters for customers who have accelerometer data and have agreed to share their data for research with Step Trainer readings.

The customer_trusted table was queried in Athena.
The following images show relevant Athena Queries to verify the correct table creation and the correct amount of data points.

![alt text](AthenaQueries/Screenshots/customer_trusted.png)

Verification, that customer_trusted really shows customer who agreed using their data.
![alt text](AthenaQueries/Screenshots/customer_trusted_verified.png)

</details>

<details>
<summary>
Curated Zone
</summary>

In the Curated Zone I created AWS Glue jobs to make further transformations, to meet the specific needs of a particular analysis. E.g. the tables were reduced to only show necessary data.

**Glue job scripts**

[customer_trusted_to_curated.py](GlueETL/Customer/customer_trusted_to_curated.py) - This script transfers customer data from the 'trusted' to 'curated' zones. Using a join on customer_trusted and accelerometer_landing, It filters for customers with Accelerometer readings and have agreed to share data with researchers.

[create_machine_learning_curated.py](GlueETL/StepTrainer/create_machine_learning_curated.py): This script is used to build aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.

The following images show relevant Athena Queries to verify the correct table creation and the correct amount of data points.

![alt text](AthenaQueries/Screenshots/machine_learning_curated.png)

</details>
