from datetime import datetime
from pydantic.typing import NoneType

import yaml
import pytest
import pytz
from rich import print
from yaml.loader import SafeLoader

from try_pipelining.data_models import (
    CTANorth,
    ScienceAlert,
    SchedulingBlock,
)
from try_pipelining.pipelines import run_pipeline, parse_tasks, parse_post_actions

from try_pipelining.post_actions import (
    Wobble,
)


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

    tasks = parse_tasks(
        science_alert=science_alert,
        site=site,
        tasks_configuration_section=processing_pipeline["tasks"],
    )

    post_actions = parse_post_actions(
        science_alert=science_alert,
        post_action_cfg=processing_pipeline["post_action"],
    )

    use_result_from = processing_pipeline["final_result_from"]

    result = run_pipeline(
        tasks=tasks,
        return_result=use_result_from,
        post_actions=post_actions,
    )

    if tasks_pass:
        assert isinstance(result, SchedulingBlock)
        print("[bold blue]--- RESULT ---")
        print(result.dict())
    else:
        assert isinstance(result, NoneType)


def test_wobble_validation():
    Wobble(offsets=[0.7, 0.7, 0.7, 0.7], angles=[0, 90, 180, 270])

    with pytest.raises(AttributeError):
        Wobble(offsets=[0.7, 0.7, 0.7], angles=[0, 90, 180, 270])
