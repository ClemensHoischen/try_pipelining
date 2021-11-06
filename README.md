[![CI](https://github.com/ClemensHoischen/try_pipelining/actions/workflows/run_tests.yml/badge.svg?branch=master)](https://github.com/ClemensHoischen/try_pipelining/actions/workflows/run_tests.yml)

# README

This is a little project to explore on how to implement dynamically assembeled pipelines.

## Plans Ideas and other stuff

### The current pipeline

The currently implemented pipeline allows to get only one single kind of result, an ObservationWindow. That should somehow be part of the pipeline definition. On could
name the pipeline accordingly and state the kind of result in the config of the pipeline.

Other pipelines may result in:

- A decision to retract a previous observation window
- A decision to update a previous observation window
- A list of observation windows for a number of nights (monitoring)
- A list of observation windows covering a larger region with differnt positions
- ...

All these kind of pipelines and their results should be representable in the
configuration of the pipeline itself.

# Pipeline Configurations

The pipelines should be easy to configure in a human-friendly way.
`YAML` seems to be good candidate for that.



