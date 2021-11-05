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


def run_pipeline(
    science_alert: ScienceAlert,
    site: Union[CTANorth],
    tasks: List[Task],
):

    task_results = {}
    for task in tasks:
        task_name = task.task_name
        task_options = task.task_options
        pipe_name = task.pipeline_name

        print(f"Processing task: {task.task_name} as pipeline: {pipe_name}")
        options = data_models.options_map[pipe_name](**task_options)
        pipe = pipelines.pipeline_map[pipe_name](science_alert, site, options)
        task_result = pipe.run()
        print("... Task done.")

        raw_filter_options = task.filter_options
        filter_opts = data_models.filter_option_map[pipe_name](**raw_filter_options)
        filtered_results = pipe.filter(result=task_result, filter_options=filter_opts)
        print("... Filtering done.")

        task_results[task_name + "Result"] = filtered_results

    return task_results


def test_pipeline():

    ALERT_DICT = {
        "coords": {"raInDeg": 262.8109, "decInDeg": 14.6481},
        "alert_time": datetime(2021, 2, 10, 2, 00, 27, 91, tzinfo=pytz.utc),
        "measured_parameters": {"count_rate": 1.2e3, "system_stable": True},
    }

    science_alert = ScienceAlert(**ALERT_DICT)
    site = CTANorth()

    TASKS = [
        Task(
            task_name="FactorialsPipeline",
            pipeline_name="FactorialsPipeline",
            task_options={"fact_n": 25},
            filter_options={"min_fact_val": 1e10},
        ),
        Task(
            task_name="ObservationWindowPipeline",
            pipeline_name="ObservationWindowPipeline",
            task_options=OPTIONS_DATA,
            filter_options=FILTER_DATA,
        ),
        Task(
            task_name="ParameterCountRate",
            pipeline_name="ParameterPipeline",
            task_options={},
            filter_options={
                "parameter_name": "count_rate",
                "parameter_requirement": 1e3,
                "parameter_comparison": "greater",
            },
        ),
        Task(
            task_name="ParameterSystemStable",
            pipeline_name="ParameterPipeline",
            task_options={},
            filter_options={
                "parameter_name": "system_stable",
                "parameter_requirement": True,
                "parameter_comparison": "equal",
            },
        ),
    ]

    task_results = run_pipeline(science_alert, site, TASKS)

    print("all the Results:")
    print(task_results)
    print("---------------------")

    pars_ok = analyse_parameter_pipe_results(task_results)
    print(f"Checking that all analysed Paramters are fulfilled ... {pars_ok}")

    if pars_ok:
        print("The earliest observation window that fulfills all filtering is:")
        print("\t", get_earliest_observation_window_from_results(task_results))

    print("No further actions...")


def get_earliest_observation_window_from_results(task_results: dict):
    windows_list = task_results["ObservationWindowPipelineResult"]

    return min([win.windows[0] for win in windows_list], key=lambda w: w.delay_hours)


def analyse_parameter_pipe_results(task_results: dict) -> bool:
    pipe_results_keys = [tr for tr in task_results if "Parameter" in tr]
    pars_ok_list = [
        task_results[pipe_results_key].parameter_ok
        for pipe_results_key in pipe_results_keys
    ]

    return all(pars_ok_list)