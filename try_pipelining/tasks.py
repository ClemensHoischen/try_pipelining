from typing import Union

from try_pipelining import parameter
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

available_tasks = {}


def register_task(cls):
    available_tasks.update({cls.__name__: cls})
    return cls


class Task:
    """Base Class for all Task Implementations.

    Implementations need to define both the run() and the filter() method."""

    def __init__(self, science_alert, site, options):
        self.science_alert = science_alert
        self.site = site
        self.options = options
        self.passed = False


@register_task
class FactorialsTask(Task):
    """Task implementation that calculates a factorial and filters based the resulting value."""

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
            self.passed = True
            return result


@register_task
class ObservationWindowTask(Task):
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

        selected_window = select_observation_window(
            filtered_windows, filter_options.window_selection
        )
        if selected_window:
            self.passed = True
            return selected_window


@register_task
class ParameterTask(Task):
    """Pipeline Implementation that only filters parameters of the alert."""

    def run(self):
        """Nothing to be done here."""
        return None

    def filter(
        self, result: None, filter_options: ParameterFilterOptions
    ) -> ParameterResult:
        """Checks that the alert parmaeter machtes the requirement specified in the filter options."""
        pars: dict = self.science_alert.measured_parameters
        passed = parameter.execute_parameter_filtering(pars, filter_options)
        if passed:
            self.passed = True
        return ParameterResult(
            parameter_name=filter_options.parameter_name,
            parameter_ok=passed,
        )
