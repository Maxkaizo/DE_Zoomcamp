# Getting things ready!

For this homework, you will need the following datasets:

- Green Taxi dataset (2019 and 2020)
- Yellow Taxi dataset (2019 and 2020)
- For Hire Vehicle dataset (2019)

Before you start

Make sure you, at least, have them in GCS with a External Table OR a Native Table - use whichever method you prefer to accomplish that (Workflow Orchestration with pandas-gbq, dlt for gcs, dlt for BigQuery, gsutil, etc)

- You should have exactly 7,778,101 records in your Green Taxi table
- You should have exactly 109,047,518 records in your Yellow Taxi table
- You should have exactly 43,244,696 records in your FHV table
- Build the staging models for green/yellow as shown in here
- Build the dimension/fact for taxi_trips joining with dim_zones as shown in here

Note: If you don't have access to GCP, you can spin up a local Postgres instance and ingest the datasets above

## Notes

I guess for this task I can use:

- Terraform, to create my gcp bucket and dataset
- Kestra, to download files from repo and upload to BigQuery (I should create the flow for fhv)
- DBT, to mantain a unique db (or view)

Let's try it

## Create a bucket and a dataset with Terraform

- I'll edit my main.tf and variables.tf files to create an environment for week 4 homework

![alt text](image.png)

- Once edit I'll create my infra with

```bash
terraform init
terraform plan
terraform apply
```
![alt text](image-4.png)

## Move files from repo to GCP Sotrage (bucket)

I'll have to start my Kestra server, run a backfill for green and yellow sets and create a flow for FHV Set.

- Check if Kestra server is running and start it if not

![alt text](image-2.png)

- Now that the server is running, I'l adjust the gcp infra definition within kestra by editing the 04_gcp_kv.yaml file (Thanks to Manuel Guerra for the reminder and his useful notes)

![alt text](image-3.png)


- I'll test the flow with only one file


At this point I encountered an issue, as I didn't know that in the terraform configuration file, the location is only used for big query (or I forgot it) and had some problems, because I left it as US, it was fixed when I changed it to us-central1

![alt text](image-5.png)

Once this was fixed, I could run fine the loading process for the first file

![alt text](image-6.png)

So now, I'm using the backfill process to extract all files for Green Taxis

Green trip data loaded succesfuly

![alt text](image-7.png)

Let's do the Yellow trip data






My next step is to load all yellow files, but I need to make sure how can I load all FHV files
My plan is to load one example file to compare the fields

















