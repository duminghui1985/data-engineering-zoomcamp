# Homework

## Question 1
- Modify the flow [08_gcp_taxi.yaml](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/flows/08_gcp_taxi.yaml). Disable purge_file to show the file size in outputs:

```yaml
- id: purge_files
    type: io.kestra.plugin.core.storage.PurgeCurrentExecutionFiles
    description: If you'd like to explore Kestra outputs, disable it.
    disabled: true
```
- Execute flow
- Check the outputs: Outputs -> extract -> outputFiles -> yellow_tripdata_2020-12.csv: find the size of the file is __128.3 MiB__

## Question 2
The variable file is "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv" with  
&ensp;&ensp;inputs.taxi: green  
&ensp;&ensp;inputs.year: 2020  
&ensp;&ensp;inputs.month: is 04  
So that the render value of var.file is __green_tripdata_2020-04.csv__

## Question 3
- Execute backfill of flow [09_gcp_taxi_scheduled.yaml](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/flows/09_gcp_taxi_scheduled.yaml) with  
Start: 2020-01-01 00:00:00  
End: 2021-01-01 00:00:00  
Taxi type: yellow  
- Open the yellow_tripdata table on GCP.
- If the table is new created: Find the "Number of rows" in Details: __24,648,499__
- Alternative: Query with SQL
```sql
SELECT 
    count(*) AS total_rows_2020
FROM 
    `{project}.{dataset}.yellow_tripdata`
WHERE 
    filename LIKE 'yellow_tripdata_2020-%.csv';
```

## Question 4
- Execute backfill of flow [09_gcp_taxi_scheduled.yaml](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/flows/09_gcp_taxi_scheduled.yaml) with   
Start: 2020-01-01 00:00:00  
End: 2021-01-01 00:00:00  
Taxi type: green  
- Open the table green_tripdata table on GCP.
- If the table is new created: Find the "Number of rows" in Details: __1,734,051__
- Alternative: Query with SQL
```sql
SELECT 
    count(*) AS total_rows_2020
FROM 
    `{project}.{dataset}.green_tripdata`
WHERE 
    filename LIKE 'green_tripdata_2020-%.csv';
```

## Question 5
-  Execute backfill of flow [09_gcp_taxi_scheduled.yaml](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/flows/09_gcp_taxi_scheduled.yaml) with   
Start: 2021-03-01 00:00:00  
End: 2020-04-01 00:00:00  
Taxi type: yellow  
- Query with SQL on GCP
```sql
SELECT 
    count(*) AS total_rows_2021_03
FROM 
    `{project}.{dataset}.yellow_tripdata`
WHERE 
    filename = 'yellow_tripdata_2021-03.csv';
```
- The result is __1925152__

## Question 6
Add __timezone__ property __America/New_York__ in triggers
```yaml
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York
    inputs:
      taxi: green

  - id: yellow_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 10 1 * *"
    timezone: America/New_York
    inputs:
      taxi: yellow
```