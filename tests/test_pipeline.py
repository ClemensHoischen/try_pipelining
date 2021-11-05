from datetime import datetime
import pytz

from try_pipelining.pipelines import run_pipeline
from try_pipelining.data_models import ScienceAlert, CTANorth, Task
from try_pipelining.parameter import analyse_parameter_pipe_results

from rich import print
from rich.console import Console


def test_pipeline():
    console = Console()
    print("\n")

    alert_dict = {
        "coords": {"raInDeg": 262.8109, "decInDeg": 14.6481},
        "alert_time": datetime(2021, 2, 10, 2, 00, 27, 91, tzinfo=pytz.utc),
        "measured_parameters": {"count_rate": 1.2e3, "system_stable": True},
    }

    # Some kind of Mock of actual ScienceAlert using the above data as input.
    science_alert = ScienceAlert(**alert_dict)

    # Location data of the observatory.
    site = CTANorth()

    window_task_options = {
        "max_zenith_deg": 60,
        "search_range_hours": 48,
        "max_sun_altitude_deg": -18.0,
        "max_moon_altitude_deg": -0.5,
        "precision_minutes": 1,
        "min_delay_minutes": 0,
        "max_delay_minutes": 24 * 60,
        "min_duration_minutes": 10,
    }

    window_filter_options = {
        "min_window_duration_hours": 0.1,
        "max_window_delay_hours": 50,
        "window_selection": "longest",
    }

    # These tasks would be defined in Science Configurations.
    tasks = [
        Task(
            task_name="FactorialsPipeline",
            pipeline_name="FactorialsPipeline",
            task_options={"fact_n": 25},
            filter_options={"min_fact_val": 1e20},
        ),
        Task(
            task_name="ObservationWindowPipeline",
            pipeline_name="ObservationWindowPipeline",
            task_options=window_task_options,
            filter_options=window_filter_options,
        ),
        Task(
            task_name="ParameterCountRate",
            pipeline_name="ParameterPipeline",
            filter_options={
                "parameter_name": "count_rate",
                "parameter_requirement": 1e3,
                "parameter_comparison": "greater",
            },
        ),
        Task(
            task_name="ParameterSystemStable",
            pipeline_name="ParameterPipeline",
            filter_options={
                "parameter_name": "system_stable",
                "parameter_requirement": True,
                "parameter_comparison": "equal",
            },
        ),
    ]

    # All tasks are executed sequentially.
    console.rule("Processing of Tasks...")
    task_results = run_pipeline(science_alert, site, tasks)

    # The results are reported in a dict.
    console.rule("[bold] Results:")
    print(task_results)
    console.rule()

    # We can analyse the results as needed. Some common things would e.g. be:
    # - Did the Parameter Tasks pass?
    pars_ok = analyse_parameter_pipe_results(task_results)
    print(
        f"[green]Checking that all analysed Paramters are fulfilled ... [/green][bold]{pars_ok}"
    )

    if pars_ok:
        # - if yes: What is the observation window that was finally selected?
        window = task_results["ObservationWindowPipelineResult"]
        print(
            f"[green]The selected observation window that passed all filtering is:[/green]"
        )
        print(window.dict())
    else:
        print("[red] No valid Observation Window.")
