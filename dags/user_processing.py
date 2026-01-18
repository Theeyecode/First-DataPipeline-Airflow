from airflow.sdk import dag , task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook


# TODO: Create_Table -> Is_API_Available -> Extract_User -> Process_User -> Store_User



@dag
def user_processing():
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY,
    firstname VARCHAR(225) NOT NULL,
    lastname VARCHAR(225) NOT NULL,
    email VARCHAR(225) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

    ) #TODO Create an airflow connection in UI with conn_id "postgres" to connect to the Postgres database

    # Verify if an API is reachable / available using a sensor

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    
    # Extract user information from the API response
    
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname":fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"]
        }
    
    # Process user information (e.g., formatting)
    @task
    def process_user(user_info):
        import csv
        from datetime import datetime
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open("/tmp/user_info.csv", mode = "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
    # Store user information into the Postgres database
    
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
            )

    process_user(extract_user(create_table >> is_api_available())) >> store_user()

# Define the DAG

user_processing()
