"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the 
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust 
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your 
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

#Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    #Define tasks
    @task(
        #Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Dataset("current_astronauts")]
    )  # Define that this task updates the `current_astronauts` Dataset
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts 
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        r = requests.get("http://api.open-notify.org/astros.json")
        
        # Debug information about the response
        print(f"Response status code: {r.status_code}")
        print(f"Response headers: {dict(r.headers)}")
        print(f"Response content type: {r.headers.get('content-type', 'Not specified')}")
        print(f"Response content length: {len(r.content)} bytes")
        print(f"Response encoding: {r.encoding}")
        print(f"Response text (first 500 chars): {r.text[:500]}")
        
        # Check if request was successful
        if r.status_code != 200:
            print(f"ERROR: Request failed with status code {r.status_code}")
            print(f"Response text: {r.text}")
            raise Exception(f"API request failed with status {r.status_code}")
        
        # Parse JSON once and store it
        try:
            response_data = r.json()
            print(f"Successfully parsed JSON. Keys in response: {list(response_data.keys())}")
        except Exception as e:
            print(f"ERROR: Failed to parse JSON - {e}")
            print(f"Raw response content: {r.content}")
            raise
            
        number_of_people_in_space = response_data["number"]
        list_of_people_in_space = response_data["people"]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an 
        Astronaut in space and the craft they are flying on from 
        the API request results of the previous task, along with a 
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    #Use dynamic task mapping to run the print_astronaut_craft task for each 
    #Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=get_astronauts() #Define dependencies using TaskFlow API syntax
    )

#Instantiate the DAG
example_astronauts()
