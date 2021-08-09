# airflow-notifications-tutorial

## Tutorial Overview
This tutorial provides a couple of example DAGs to show how to use advanced notifications in Airflow. It is meant to complement this guide on [error notifications in Airflow](https://www.astronomer.io/guides/error-notifications-in-airflow) and our Airflow notifications webinar (link coming soon).

Specific advanced DAGs in this repo include:

 - `sla-dag` to show use of Airflow SLAs
 - `data-quality-dag` to show implementation of in-DAG data quality checks to be used in conjunction with notifications
 

## Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:

 1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
 2. Clone this repo somewhere locally and navigate to it in your terminal
 3. Initialize an Astronomer project by running `astro dev init`
 4. Start Airflow locally by running `astro dev start`
 5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there
