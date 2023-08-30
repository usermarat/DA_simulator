# Data Analyst simulator

This repo contains of the tasks completed at the DA simulator course provided by the Karpov courses (https://karpov.courses/simulator).

All of the tasks are truly practice oriented - the whole program is built in form of mock internship at the newly established IT start-up. The main product is an app with a news feed and a messaging functionality. By the legend at the company there is no any data analysis system yet; the only approach is just gathering all the basic information about the users and their in-app activity logs (which where artificially generated in real time through the whole lifetime of the app).

As a newly hired data analyst intern I would have to go through the whole process of organizing all the data analysis processes from the scratch such as:
1. building essential dashboards considering:
* product's (both news feed and messaging parts of the app) and user's overall data understanding,
* operating performance and management's KPI monitoring;
2. ad hoc product metric analysis, including:
* ads and organic audience traffic retention rate analysis,
* marketing campaign results' analysis,
* active audience sudden drop investigation;
3. A/B testing for the new feature implementation, i.e.:
* prior A/A testing,
* A/B testing with different approaches, including bootstrap, bucketization, linearization;
4. ETL pipeline building using Airflow for automation;
5. app product metrics' reporting automation using:
* telegram bot API (constructing bot and sending report text and plots),
* Airflow for daily running the scripts;
6. alerting system for the real-time anomaly detection modeling and deploying.

# Tech stack used during the course:
* python,
* pandas,
* scipy,
* sql,
* Click House,
* Redash,
* SuperSet,
* Airflow,
* Jupiterhub,
* Gitlab,
* telegram bot API.