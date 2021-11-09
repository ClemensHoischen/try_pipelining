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
    ObservationBlock,
    ParameterOptions,
    ParameterFilterOptions,
)
from try_pipelining.pipelines import (
    run_pipeline,
    parse_tasks,
    parse_post_actions,
    match_science_configs,
    execute_pipeline_from_cfg,
)

from try_pipelining.tasks import Task

from try_pipelining.post_actions import Wobble, PostAction


alert_dict = {
    "unique_id": "ivo://nasa.gcn.gov/SWIFT#BAT_GRB_Pos#1234567-1337",
    "coords": {"raInDeg": 262.8109, "decInDeg": 14.6481},
    "alert_time": datetime(2021, 2, 10, 2, 00, 27, 91, tzinfo=pytz.utc),
    "measured_parameters": {
        "count_rate": 12300,
        "system_stable": True,
        "noise": 0.5,
    },
}


@pytest.mark.parametrize(
    "system_stable, count_rate, noise_value, tasks_pass",
    [(True, 1.2e3, 5.2, True), (False, 0.9e3, 20, False)],
)
def test_pipeline_from_yaml(
    noise_value: float, count_rate: float, system_stable: bool, tasks_pass: bool
):
    alert_dict = {
        "unique_id": "ivo://nasa.gcn.gov/SWIFT#BAT_GRB_Pos#1234567-1337",
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
        sb = result["CreateWobbleSchedulingBlock"]
        assert isinstance(sb, SchedulingBlock)
        obs = result["CreateObservationBlocks"]
        for ob in obs:
            assert isinstance(ob, ObservationBlock)

        print("[bold blue]--- RESULTS ---")
        print(f"[blue] got {type(sb).__name__}:")
        print(sb.dict())
        print(f"[blue] got {len(obs)} {type(obs[0]).__name__}s:")
        for i, r in enumerate(obs):
            assert isinstance(r, ObservationBlock)
            print(f"[bold blue] {type(r).__name__} {i}:")
            print(r.dict())

    else:
        assert isinstance(result, NoneType)

    print("[bold blue]--------------")


def test_wobble_validation():
    Wobble(offsets=[0.7, 0.7, 0.7, 0.7], angles=[0, 90, 180, 270])

    with pytest.raises(AttributeError):
        Wobble(offsets=[0.7, 0.7, 0.7], angles=[0, 90, 180, 270])


def test_match_science_alert():
    # Some kind of Mock of actual ScienceAlert using the above data as input.
    science_alert = ScienceAlert(**alert_dict)

    path_to_configs = "configs/"
    applicable_configs = match_science_configs(science_alert, path_to_configs)
    assert len(applicable_configs) == 1

    matched_cfg = applicable_configs[0]
    print(matched_cfg["pipeline"])


def test_run_from_api():
    sci_alert = ScienceAlert(**alert_dict)
    site = CTANorth()
    cfgs = match_science_configs(sci_alert, "configs")
    for cfg in cfgs:
        results = execute_pipeline_from_cfg(sci_alert, site, cfg["pipeline"])
        print(results)


def test_tasks_and_actions():
    # Test usage with ill-defined task name and options
    with pytest.raises(KeyError):
        Task(
            science_alert=ScienceAlert(**alert_dict),
            site=CTANorth(),
            task_name="a_task",
            task_type="a_task",
            task_options={"opt": 1},
            filter_options={"filt": 2},
        )
    # test usage with ill-defined action type and options
    with pytest.raises(KeyError):
        PostAction(
            science_alert=ScienceAlert(**alert_dict),
            action_type="an_action",
            action_options={"option": 1},
        )

    # test usage with non-matching task and options
    with pytest.raises(AssertionError):
        opts_dict = {
            "parameter_name": "count_rate",
            "parameter_requirement": 1.0e3,
            "parameter_comparison": "greater",
        }
        par_opts = ParameterFilterOptions(**opts_dict)
        Task(
            science_alert=ScienceAlert(**alert_dict),
            site=CTANorth(),
            task_name="ObservationWindowTask",
            task_type="ObservationWindowTask",
            task_options=ParameterOptions(),
            filter_options=par_opts,
        )

    # Test incomplete Task implementation
    class NewTask(Task):
        def new_run_method(self):
            # say we implemented the logic in the wrong method.
            pass

    nt = NewTask(
        science_alert=ScienceAlert(**alert_dict),
        site=CTANorth(),
        task_name="Parameter",
        task_type="ParameterTask",
        task_options=ParameterOptions(),
        filter_options=ParameterFilterOptions(**opts_dict),
    )
    with pytest.raises(NotImplementedError):
        nt.run()
    with pytest.raises(NotImplementedError):
        nt.filter(result={})
