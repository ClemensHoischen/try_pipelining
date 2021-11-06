from typing import List, Any

from rich.panel import Panel
from rich.progress import Progress

from try_pipelining import data_models
from try_pipelining.tasks import available_tasks
from try_pipelining.data_models import SchedulingBlock, ScienceAlert


available_post_actions = {}


def register_post_action(func):
    available_post_actions.update({func.__name__: func})
    return func


@register_post_action
def create_wobble_scheduling_block(science_alert, task_result, options):
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


class MyProgress(Progress):
    def get_renderables(self):
        yield Panel(self.make_tasks_table(self.tasks))


def run_pipeline(
    science_alert: data_models.ScienceAlert,
    site: data_models.CTANorth,
    tasks: List[data_models.TaskConfig],
    return_result: str,
):
    """The Actial Pipeline function."""

    with MyProgress() as progress:
        progress_tasks = [
            progress.add_task(f"[green]Task: {task.task_name}", total=3)
            for task in tasks
        ]

        task_results = {}
        tasks_passed = []
        for i, task in enumerate(tasks):
            # --- Setup of the Task ---
            task_name = task.task_name
            task_options = task.task_options
            task_type = task.task_type
            progress.update(progress_tasks[i], advance=1)

            # --- Execute the task---
            options = data_models.task_options[task_type](**task_options)
            t = available_tasks[task_type](science_alert, site, options)
            task_result = t.run()
            progress.update(progress_tasks[i], advance=1)

            # --- Execute the filtering ---
            raw_filter_options = task.filter_options
            filter_opts = data_models.filter_options[task_type](**raw_filter_options)
            filtered_results = t.filter(result=task_result, filter_options=filter_opts)

            # --- Add to the Results Dict ---
            task_results[task_name + "Result"] = filtered_results
            progress.update(progress_tasks[i], advance=1)
            tasks_passed.append(t.passed)

        if False not in tasks_passed:
            return task_results[return_result + "Result"]
