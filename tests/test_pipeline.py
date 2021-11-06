from datetime import datetime

import yaml
import pytest
import pytz
from rich import print
from rich.console import Console
from yaml.loader import SafeLoader

from try_pipelining.data_models import (
    CTANorth,
    ScienceAlert,
    TaskConfig,
    SchedulingBlock,
)
from try_pipelining.pipelines import run_pipeline, execute_post_action
from try_pipelining.observation_windows import ObservationWindow


@pytest.mark.parametrize(
    "system_stable, count_rate, noise_value, tasks_pass",
    [(True, 1.2e3, 5.2, True), (False, 0.9e3, 20, False)],
)
def test_pipeline_from_yaml(
    noise_value: float, count_rate: float, system_stable: bool, tasks_pass: bool
):
    alert_dict = {
        "coords": {"raInDeg": 262.8109, "decInDeg": 14.6481},
        "alert_time": datetime(2021, 2, 10, 2, 00, 27, 91, tzinfo=pytz.utc),
        "measured_parameters": {
            "count_rate": count_rate,
            "system_stable": system_stable,
            "noise": noise_value,
        },
    }

    # Some kind of Mock of actual ScienceAlert using the above data as input.
    science_alert = ScienceAlert(**alert_dict)

    # Location data of the observatory.
    site = CTANorth()

    config_file_path = "configs/pipeline_config.yaml"
    with open(config_file_path, "rb") as confg_file:
        config_data = yaml.load(confg_file, Loader=SafeLoader)

    processing_pipeline = config_data["pipeline"]
    use_result_from = processing_pipeline["final_result_from"]

    tasks = [
        TaskConfig(**task_spec) for _, task_spec in processing_pipeline["tasks"].items()
    ]

    result = run_pipeline(
        science_alert=science_alert,
        site=site,
        tasks=tasks,
        return_result=use_result_from,
    )
    assert isinstance(result, ObservationWindow) == tasks_pass
    if not tasks_pass:
        return

    post_action_result = execute_post_action(
        science_alert=science_alert,
        task_result=result,
        post_action_options=processing_pipeline["post_action"]["post_action_options"],
        post_action_name=processing_pipeline["post_action"]["post_action_name"],
    )
    print("SchedulingBlock:", post_action_result)

    assert isinstance(post_action_result, SchedulingBlock)
