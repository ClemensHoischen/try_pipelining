from typing import List, Any

from rich.progress import track
from rich.tree import Tree
from rich import print

from try_pipelining import data_models
from try_pipelining.data_models import available_task_options, available_filter_options
from try_pipelining.tasks import available_tasks
from try_pipelining.data_models import SchedulingBlock, ScienceAlert


available_post_actions = {}


def register_post_action(func):
    available_post_actions.update({func.__name__: func})
    return func


@register_post_action
def create_wobble_scheduling_block(
    science_alert: ScienceAlert, task_result: Any, options: dict
):
    sb_dict = {
        "coords": science_alert.coords,
        "time_constraints": {
            "start_time": task_result.start_time,
            "end_time": task_result.end_time,
        },
        "wobble_options": options["wobble"],
        "proposal": options["proposal"],
    }
    return SchedulingBlock(**sb_dict)


def execute_post_action(
    science_alert: ScienceAlert,
    task_result: Any,
    post_action_name: str,
    post_action_options: dict,
):
    return available_post_actions[post_action_name](
        science_alert, task_result, post_action_options
    )


def run_pipeline(
    science_alert: data_models.ScienceAlert,
    site: data_models.CTANorth,
    task_cfgs: List[data_models.TaskConfig],
    return_result: str,
):
    """The Actial Pipeline function."""

    # Translate from task configs to Task Implementations
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

    task_results = {}
    tasks_passed = []
    tasks_report = []

    for t in track(
        tasks,
        description="[bold blue]+Running Tasks...",
        total=len(task_cfgs),
        style="bar.back",
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

    if False not in tasks_passed:
        return task_results[return_result + "Result"]
