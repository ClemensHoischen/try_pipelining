from datetime import datetime
import pytz

from try_pipelining.data_models import ScienceAlert, Darkness, CTANorth
from typing import List, Union
from pydantic import BaseModel
from rich import print

from try_pipelining import data_models
from try_pipelining import pipelines


OPTIONS_DATA = {
    "max_zenith_deg": 60,
    "search_range_hours": 48,
    "sky_brightness": Darkness(),
    "precision_minutes": 1,
    "min_delay_minutes": 0,
    "max_delay_minutes": 24 * 60,
    "min_duration_minutes": 10,
}


class Task(BaseModel):
    task_name: str
    task_options: dict


ALERT = {
    "coords": {"raInDeg": 262.8109, "decInDeg": 14.6481},
    "alert_time": datetime(2021, 2, 10, 2, 00, 27, 91, tzinfo=pytz.utc),
}


def run_tasks(
    science_alert: ScienceAlert,
    site: Union[CTANorth],
    tasks: List[Task],
):

    task_results = {}
    for task in tasks:
        task_name = task.task_name
        task_options = task.task_options

        options = data_models.options_map[task_name.replace("Pipeline", "Options")](
            **task_options
        )
        pipe = pipelines.pipeline_map[task_name](science_alert, site, options)
        result = pipe.run()

        task_results[task_name + "Result"] = result

    return task_results


def test_pipeline():

    ALERT_DICT = {
        "coords": {"raInDeg": 262.8109, "decInDeg": 14.6481},
        "alert_time": datetime(2021, 2, 10, 2, 00, 27, 91, tzinfo=pytz.utc),
    }

    science_alert = ScienceAlert(**ALERT_DICT)
    site = CTANorth()

    TASKS = [
        Task(
            task_name="FactorialsPipeline",
            task_options={"fact_n": 25},
        ),
        Task(
            task_name="ObservationWindowPipeline",
            task_options=OPTIONS_DATA,
        ),
    ]

    task_results = run_tasks(science_alert, site, TASKS)

    print(task_results)