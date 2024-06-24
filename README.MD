

pip install apache-airflow-providers-snowflake


https://forum.astronomer.io/t/managing-snowflake-tasks-and-tree-tasks-with-airflow/2825



## DOCKER DESKTOP



## DOCKER CMD

### Start
-- docker-compose up

### Remove containers based on status:
-- docker rm -v $(docker ps --filter status=exited -q)


### o clean out all containers on my development machine:
-- docker rm -v -f $(docker ps -qa)



## AIRFLOW : CONNEXTION SNOWFLAKE



## NOTE