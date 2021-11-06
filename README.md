[![CI](https://github.com/ClemensHoischen/try_pipelining/actions/workflows/run_tests.yml/badge.svg?branch=master)](https://github.com/ClemensHoischen/try_pipelining/actions/workflows/run_tests.yml)

# README

This is a little project to explore on how to implement dynamically assembeled pipelines.
Currently this project is able to do:

- Read a pipeline configuration YAML file
- Execute the specified tasks and filter the results according to the configuration
- perform a post-action step

![image](https://drive.google.com//uc?export=view&id=1VQqwRFlStjItjjocyjI-8x3a-jJBTwPP)

This software uses:

- `rich` for pretty output
- `poetry` for bulding and dependency management
- `black` format

## Plans Ideas and other stuff

### Integration of Pipeline execution and Post-Action

Currently the pipeline of tasks and the post-action are
called independently of each other.

### More Tasks

Only two actually useful tasks are implemented right now (`ObservationWindowTask` and `ParameterTask`). Many other Tasks will be needed:

- `DetermineUpdateTask`
- `DetermineRetractionTask`
- `DetermineMonitoringStrategyTask` (e.g. with options like `best_zenith`, `n_nights`, `hours_per_night`)

### Actually use VOEvents as alerts

The simple mock alerts will not be sufficient for ever.
VOEvent is the international standard for transient alerts.

### More Pipelines

Pipelines could result in:

- A decision to trigger observations (currently implemented)
- A decision to retract a previous observation window
- A decision to update a previous observation window
- A list of observation windows for a number of nights (monitoring)
- A list of observation windows covering a larger region with differnt positions
- ...

# Pipeline Configurations

The pipelines should be easy to configure in a human-friendly way.
`YAML` seems to be good candidate for that.

A pipeline definition could look like [this](configs/pipeline_config.yaml).
Nicely annotated and humanly readable.
