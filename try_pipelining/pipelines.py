from typing import List

import os
import yaml
from yaml.loader import SafeLoader

from rich.progress import track
from rich.tree import Tree
from rich import print


from try_pipelining.data_models import (
    ScienceAlert,
    CTANorth,
    TaskConfig,
    ScienceAlert,
    SchedulingBlock,
    ObservationBlock,
    available_task_options,
    available_filter_options,
)
from try_pipelining.post_actions import (
    PostAction,
    available_post_actions,
    available_post_action_options,
)
from try_pipelining.tasks import available_tasks, Task


def match_science_configs(science_alert: ScienceAlert, path_to_configs: str):
    all_cfgs = os.listdir(path_to_configs)
    cfgs = [path_to_configs + "/" + cfg for cfg in all_cfgs]

    matched_cfg_data_list = []
    for cfg in cfgs:
        with open(cfg, "rb") as confg_file:
            config_data = yaml.load(confg_file, Loader=SafeLoader)

        matching_reqs = config_data["alert_matching"]
        for alert_type in matching_reqs:
            print(alert_type, matching_reqs[alert_type])
            req_keys = matching_reqs[alert_type]["required_keys"]
            if all([key in science_alert.unique_id for key in req_keys]):
                matched_cfg_data_list.append(config_data)

    return matched_cfg_data_list


def parse_tasks(
    science_alert: ScienceAlert, site: CTANorth, tasks_configuration_section: dict
) -> List[Task]:

    task_cfgs = [
        TaskConfig(task_name=task_name, **task_spec)
        for task_name, task_spec in tasks_configuration_section.items()
    ]

    tasks = [
        available_tasks[t.task_type](
            science_alert=science_alert,
            site=site,
            task_name=t.task_name,
            task_type=t.task_type,
            task_options=available_task_options[t.task_type](**t.task_options),
            filter_options=available_filter_options[t.task_type](**t.filter_options),
        )
        for t in task_cfgs
    ]
    return tasks


def parse_post_actions(
    science_alert: ScienceAlert, post_action_cfg: dict
) -> List[PostAction]:
    post_actions = [
        available_post_actions[pa](
            science_alert=science_alert,
            action_type=pa,
            action_options=available_post_action_options[pa](**post_action_cfg[pa]),
        )
        for pa in post_action_cfg
    ]
    return post_actions


def execute_pipeline_from_cfg(
    science_alert: ScienceAlert, site: CTANorth, pipeline_cfg: dict
):
    tasks = parse_tasks(
        science_alert=science_alert,
        site=site,
        tasks_configuration_section=pipeline_cfg["tasks"],
    )

    post_actions = parse_post_actions(
        science_alert=science_alert,
        post_action_cfg=pipeline_cfg["post_action"],
    )

    use_result_from = pipeline_cfg["final_result_from"]

    results = run_pipeline(
        tasks=tasks,
        return_result=use_result_from,
        post_actions=post_actions,
    )

    try:
        sb = results["CreateWobbleSchedulingBlock"]
        assert isinstance(sb, SchedulingBlock)
        obs = results["CreateObservationBlocks"]
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

    except Exception as e:
        print("No valid results")
        raise e

    print("[bold blue]--------------")
    return results


def run_pipeline(
    tasks: List[Task],
    return_result: str,
    post_actions: List[PostAction],
):
    """The Actial Pipeline function."""

    # Translate from task configs to Task Implementations

    task_results = {}
    tasks_passed = []
    tasks_report = []

    for t in track(
        tasks,
        description="[bold blue]+Running Tasks...",
        total=len(tasks),
    ):
        # --- run and filter the result ---
        filtered_results = t.filter(result=t.run())

        # --- Add to the Results Dict ---
        task_results[t.task_name] = filtered_results
        tasks_passed.append(t.passed)

        rep_task = f"[bold green]PASS" if t.passed else f"[bold red]FAIL"
        tasks_report.append(f"{t.task_name} - {rep_task}")

    task_tree = Tree("[bold Blue]+Tasks report:", highlight=True)
    [task_tree.add(rep) for rep in tasks_report]
    print(task_tree)

    if False in tasks_passed:
        print(
            "[bold red]Some Tasks failed... -> No valid result returned from Pipeline."
        )
        print(
            "[bold red]Nothing more do be done here ... ",
            ":frowning_face_with_open_mouth:",
        )
        return

    # The task result that is specified to be used further.
    result = task_results[return_result]

    post_action_tree = Tree("[bold Blue]+Post-action report:", highlight=True)
    # a dict of the post-action results for logging and reporting purposes.
    post_action_results = {return_result: result}
    for post_action in track(
        post_actions,
        description="[bold blue]+Executing Post-action",
        total=len(post_actions),
    ):
        # results are chained in order of post action specificiation in the configuration
        result = post_action.run(task_result=result)
        post_action_results.update({post_action.action_type: result})
        post_action_tree.add(post_action.action_type + "[bold green] DONE")

    print(post_action_tree)
    return post_action_results
