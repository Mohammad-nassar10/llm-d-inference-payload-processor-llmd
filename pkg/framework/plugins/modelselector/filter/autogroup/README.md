# `auto` Model Name Filter

When the `model` name is set to auto restricts the candidate models to a mode name from a set (group) of models that are defined in the filter configuration.
The filter is configured with a list of groups, each group has a name, and a list of models for the group.

It is registered as type `auto-group-model-name-filter` and runs as a modelselector filter.

When the model name is set to `auto/group-name` the filter matches the requested `group-name` against the list of groups defined in the filter and populate the candidate models with the matching modes from the datalayer.
In case the model name is set to `auto` only or is missing, it populates all the models from the data layer.

## What it does

1. Reads the `model` field from the request body.
3. If the field is absent, an empty string or `auto`, all incoming candidates are kept.
3. If the `model` field is fomatted with the prefix `auto/` ( i.e., `auto/group-name`), it extract `group-name` and looks up the group-name in the filter parameters. All candidate model names from the data layer that also appear in the group are kept.
4. If the intersection is empty or the field is malformed (not a string), the filter returns no candidates and the pipeline rejects the request with HTTP 429.

## Inputs consumed

- The `model` field of the request body.
- The candidate model list passed in by the pipeline.
- A set of groups with the associated models

## Example configuration

```yaml
plugins:
- type: auto-group-model-name-filter
  parameters:
    qwen3models:                # The group name
      - qwen3-8b                # Model name in the group 
      - qwen3-32b               # Model name in the group
```
The following configuration for the plugin defines a group `qwen3models` with two models `qwen3-8b` and `qwen3-32b`. The list of models is used to filter the candidates when the request has the `model` set to `auto/qwen3models`.
