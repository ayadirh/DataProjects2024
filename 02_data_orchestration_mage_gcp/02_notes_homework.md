## Data Orchestration - Mage, Google Cloud, Postgres

## Week 2 Homework 

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

### Questions & Answers

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns


## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 _and_ the trip distance is greater than zero, how many rows are left?

* 139,370 rows


## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`


## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1 or 2


## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 4


## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?
