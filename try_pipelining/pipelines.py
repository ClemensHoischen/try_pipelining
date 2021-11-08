from typing import List

from rich.progress import track
from rich.tree import Tree
from rich import print


from try_pipelining.data_models import (
    ScienceAlert,
    CTANorth,
    TaskConfig,
    ScienceAlert,
    available_task_options,
    available_filter_options,
)
from try_pipelining.post_actions import (
    PostAction,
    available_post_actions,
    available_post_action_options,
)
from try_pipelining.tasks import available_tasks, Task


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
        task_results[t.task_name + "Result"] = filtered_results
        tasks_passed.append(t.passed)

        rep_task = f"[bold green]PASS" if t.passed else f"[bold red]FAIL"
        tasks_report.append(f"{t.task_name} - {rep_task}")

    tree = Tree("[bold Blue]+Tasks report:", highlight=True)
    [tree.add(rep) for rep in tasks_report]
    print(tree)

    if False in tasks_passed:
        print("[bold red]Some Tasks failed.\n ... No result returned from Pipeline.")
        return

    result = task_results[return_result + "Result"]
    for post_action in track(
        post_actions,
        description="[bold blue]+Executing Post-action",
        total=len(post_actions),
    ):
        result = post_action.run(task_result=result)

    return result
