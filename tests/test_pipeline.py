from datetime import datetime
import pytz

from try_pipelining.data_models import ScienceAlert, CTANorth, Task
from try_pipelining.parameter import analyse_parameter_pipe_results
from typing import List, Union

from rich import print
from rich.console import Console
from rich.progress import Progress
from rich.panel import Panel

from try_pipelining import data_models
from try_pipelining import pipelines

console = Console()


class MyProgress(Progress):
    def get_renderables(self):
        yield Panel(self.make_tasks_table(self.tasks))


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

FILTER_DATA = {
    "min_window_duration_hours": 0.1,
    "max_window_delay_hours": 50,
    "window_selection": "longest",
}


def run_pipeline(
    science_alert: ScienceAlert,
    site: Union[CTANorth],
    tasks: List[Task],
):

    with MyProgress() as progress:
        progress_tasks = [
            progress.add_task(f"[green]Task: {task.task_name}", total=3)
            for task in tasks
        ]

        task_results = {}
        for i, task in enumerate(tasks):
            task_name = task.task_name
            task_options = task.task_options
            pipe_name = task.pipeline_name
            progress.update(progress_tasks[i], advance=1)

            # print(f"Processing task: {task.task_name} as pipeline: {pipe_name}")
            options = data_models.options_map[pipe_name](**task_options)
            pipe = pipelines.pipeline_map[pipe_name](science_alert, site, options)
            task_result = pipe.run()
            progress.update(progress_tasks[i], advance=1)
            # print("... Task done.")

            print(task_result)

            raw_filter_options = task.filter_options
            filter_opts = data_models.filter_option_map[pipe_name](**raw_filter_options)
            filtered_results = pipe.filter(
                result=task_result, filter_options=filter_opts
            )
            # print("... Filtering done.")

            task_results[task_name + "Result"] = filtered_results
            progress.update(progress_tasks[i], advance=1)

        return task_results


def test_pipeline():
    print("\n")
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
            filter_options={"min_fact_val": 1e20},
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
                "parameter_requirement": False,
                "parameter_comparison": "equal",
            },
        ),
    ]
    console.rule("Processing of Tasks...")
    task_results = run_pipeline(science_alert, site, TASKS)

    console.rule("[bold] Results:")
    print(task_results)
    console.rule()

    pars_ok = analyse_parameter_pipe_results(task_results)
    print(
        f"[green]Checking that all analysed Paramters are fulfilled ... [/green][bold]{pars_ok}"
    )

    if pars_ok:
        window = task_results["ObservationWindowPipelineResult"]
        print(
            f"[green]The earliest observation window that fulfills all filtering is:[/green]"
        )
        print(window.dict())
    else:
        print("[red] No valid Observation Window.")
