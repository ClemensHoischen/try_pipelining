from datetime import datetime
import pytz

from try_pipelining.data_models import ScienceAlert, CTANorth, Task
from typing import List, Union
from rich import print

from try_pipelining import data_models
from try_pipelining import pipelines


OPTIONS_DATA = {
    "max_zenith_deg": 60,
    "search_range_hours": 48,
    "max_sun_altitude_deg": -18.0,
    "max_moon_altitude_deg": -0.5,
    "precision_minutes": 1,
    "min_delay_minutes": 0,
    "max_delay_minutes": 24 * 60,
    "min_duration_minutes": 10,
}

FILTER_DATA = {"min_window_duration_hours": 1.1, "max_window_delay_hours": 1.2}


ALERT = {
    "coords": {"raInDeg": 262.8109, "decInDeg": 14.6481},
    "alert_time": datetime(2021, 2, 10, 2, 00, 27, 91, tzinfo=pytz.utc),
}


def run_pipeline(
    science_alert: ScienceAlert,
    site: Union[CTANorth],
    tasks: List[Task],
):

    task_results = {}
    for task in tasks:
        task_name = task.task_name
        task_options = task.task_options
        print(f"Processing task: {task.task_name}")

        options = data_models.options_map[task_name](**task_options)
        pipe = pipelines.pipeline_map[task_name](science_alert, site, options)
        task_result = pipe.run()
        print("... Task done.")

        raw_filter_options = task.filter_options
        filter_opts = data_models.filter_option_map[task_name](**raw_filter_options)
        filtered_results = pipe.filter(task_result, filter_opts)
        print("... Filtering done.")

        task_results[task_name + "Result"] = filtered_results

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
            filter_options={"min_fact_val": 1e10},
        ),
        Task(
            task_name="ObservationWindowPipeline",
            task_options=OPTIONS_DATA,
            filter_options=FILTER_DATA,
        ),
    ]

    task_results = run_pipeline(science_alert, site, TASKS)

    print("The earliest observation window that fulfills all filtering is:")
    print(get_earliest_observation_window_from_results(task_results))


def get_earliest_observation_window_from_results(task_results: dict):
    windows_list = task_results["ObservationWindowPipelineResult"]

    return min([win.windows[0] for win in windows_list], key=lambda w: w.delay_hours)
