from typing import List, Union

from rich.panel import Panel
from rich.progress import Progress

from try_pipelining import data_models, parameter
from try_pipelining.data_models import (
    FactorialsFilterOptions,
    FactorialsTaskResult,
    ObservationWindowFilterOptions,
    ObservationWindowTaskResult,
    ParameterFilterOptions,
    ParameterResult,
)
from try_pipelining.factorials import factorial
from try_pipelining.observation_windows import (
    ObservationWindow,
    calculate_observation_windows,
    select_observation_window,
    setup_night_timerange,
    setup_nights,
)


class MyProgress(Progress):
    def get_renderables(self):
        yield Panel(self.make_tasks_table(self.tasks))


def run_pipeline(
    science_alert: data_models.ScienceAlert,
    site: data_models.CTANorth,
    tasks: List[data_models.Task],
):
    """ The Actial Pipeline function. """

    with MyProgress() as progress:
        progress_tasks = [
            progress.add_task(f"[green]Task: {task.task_name}", total=3)
            for task in tasks
        ]

        task_results = {}
        for i, task in enumerate(tasks):
            # --- Setup of the Task ---
            task_name = task.task_name
            task_options = task.task_options
            pipe_name = task.pipeline_name
            progress.update(progress_tasks[i], advance=1)

            # --- Execute the task---
            options = data_models.options_map[pipe_name](**task_options)
            pipe = pipeline_map[pipe_name](science_alert, site, options)
            task_result = pipe.run()
            progress.update(progress_tasks[i], advance=1)

            # --- Execute the filtering ---
            raw_filter_options = task.filter_options
            filter_opts = data_models.filter_option_map[pipe_name](**raw_filter_options)
            filtered_results = pipe.filter(
                result=task_result, filter_options=filter_opts
            )

            # --- Add to the Results Dict ---
            task_results[task_name + "Result"] = filtered_results
            progress.update(progress_tasks[i], advance=1)

        return task_results


class Pipeline:
    """Base Class for all Pipeline Implementations.

    Implementations need to define both the run() and the filter() method."""

    def __init__(self, science_alert, site, options):
        self.science_alert = science_alert
        self.site = site
        self.options = options


class FactorialPipeline(Pipeline):
    """Pipeline implementation that calculates a factorial and filters based the resulting value."""

    def run(self):
        """Calculation of the factorial."""
        return FactorialsTaskResult(factorial_result=factorial(self.options.fact_n))

    def filter(
        self, result: FactorialsTaskResult, filter_options: FactorialsFilterOptions
    ) -> Union[FactorialsTaskResult, None]:
        """Filtering the calculated factorial."""
        assert isinstance(result, FactorialsTaskResult)
        assert isinstance(filter_options, FactorialsFilterOptions)

        if result.factorial_result > filter_options.min_fact_val:
            return result


class ObservationWindowPipeline(Pipeline):
    """Pipeline Implementation that calculates observation Windows and filters them."""

    def run(self):
        """Calculation of the Observation Windows according to the options."""
        nights = setup_nights(self.science_alert, self.options, self.site)
        testable_dates_nightlist = [
            setup_night_timerange(night, self.options) for night in nights
        ]
        observation_windows = calculate_observation_windows(
            self.science_alert, self.options, self.site, testable_dates_nightlist
        )
        return ObservationWindowTaskResult(windows=observation_windows)

    def filter(
        self,
        result: ObservationWindowTaskResult,
        filter_options: ObservationWindowFilterOptions,
    ) -> Union[ObservationWindow, None]:
        """Filters and selectes Observation Windows according to filtering options."""
        assert isinstance(result, ObservationWindowTaskResult)
        assert isinstance(filter_options, ObservationWindowFilterOptions)

        filtered_windows = []
        for window in result.windows:
            delay_ok = window.delay_hours < filter_options.max_window_delay_hours
            duration_ok = (
                window.duration_hours > filter_options.min_window_duration_hours
            )

            if delay_ok and duration_ok:
                filtered_windows.append(window)

        return select_observation_window(
            filtered_windows, filter_options.window_selection
        )


class ParameterPipeline(Pipeline):
    """Pipeline Implementation that only filters parameters of the alert."""

    def run(self):
        """Nothing to be done here."""
        return None

    def filter(
        self, result: None, filter_options: ParameterFilterOptions
    ) -> ParameterResult:
        """Checks that the alert parmaeter machtes the requirement specified in the filter options."""
        pars: dict = self.science_alert.measured_parameters
        return ParameterResult(
            parameter_name=filter_options.parameter_name,
            parameter_ok=parameter.execute_parameter_filtering(pars, filter_options),
        )


pipeline_map = {
    "FactorialsPipeline": FactorialPipeline,
    "ObservationWindowPipeline": ObservationWindowPipeline,
    "ParameterPipeline": ParameterPipeline,
}
