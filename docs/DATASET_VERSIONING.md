# Dataset Versioning

## Current versioning model in the codebase

The repository now versions research artifacts primarily through explicit ids rather than the older `dataset_versions` wording.

Key identities in current research flow:

- `snapshot_id`
- `split_id`
- `dataset_id`
- `backtest_id`
- `experiment_id`

## Practical meaning

### `snapshot_id`
Identifies the research snapshot or source selection used to build a dataset.

### `split_id`
Defines the train/valid/test windows.

### `dataset_id`
Identifies a concrete built dataset in `research_training_dataset`.

### `backtest_id`
Identifies one concrete evaluation run in `research_backtest`.

### `experiment_id`
Identifies the orchestrated run persisted by `run_research_experiment.py`.

## Reproducibility rule

A backtest is only truly reproducible if it keeps all of the following stable:

- source canonical tables
- snapshot
- split
- dataset id
- label sanitization settings
- backtest transaction costs and thresholds

## Current manifest flow

```text
snapshot_id + split_id
-> dataset_id
-> backtest_id
-> experiment_id
```

## Why older wording is stale

Earlier docs referred to:
- `training_dataset_daily`
- `dataset_versions`

The current implementation is more explicit and operational:
- frozen rows live in `research_training_dataset`
- experiment metadata is persisted by the research experiment repository
- ids are carried explicitly through the pipeline rather than inferred from a generic dataset-version object
