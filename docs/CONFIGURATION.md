# Config API

This document enumerates the different configuration options that can be set in `sunbeam.toml`.

**Table of Contents**
1. [`[config]`](#config)
2. [`[stage_data_source]`](#stage_data_source)
3. [`[ingress_data_source]`](#ingress_data_source)

## `[config]`

The `config` section contains some high-configuration.

1. `events_description_file`: Required. Should be the name of the file in the `config/` folder that contains descriptions of events that Sunbeam should process.
2. `ingress_description_file`: Required. Should be a string containing the filename of the file in the `config/` folder that contains descriptions of targets to be marshalled into Sunbeam.
3. `stages_to_run`: Required. Should be a list of strings where each string element is the name of a stage that should be run. 

```toml
[config]
events_description_file = "events.toml"
ingress_description_file = "ingress.toml"
stages_to_run = [
    "energy"
]
```


## `[stage_data_source]`

Configuration options relating to the data source that Sunbeam stages will store their data.

1. `data_source_type`: Required. Should be a string containing name of the data source that is to be used. _[`MongoDBDataSource`/`FSDataSource`]_

```toml
[stage_data_source]
data_source_type = "MongoDBDataSource"
```


### `[stage_data_source.FSDataSource]`

Options for `FSDataSource` when used as the stage data source.

1. `fs_root`: Required. The name of the directory which is to serve as the root of the Sunbeam filesystem. The directory must already exist.

```toml
[stage_data_source.FSDataSource]
fs_root = "fs_data"
```


### `[stage_data_source.MongoDBDataSource]`

No options for `MongoDBDataSource` when used as stage data source, yet.

## `[ingress_data_source]`

1. Required. Should be a string containing name of the data source that is to be used for ingress. _[`MongoDBDataSource`/`FSDataSource`/`InfluxDBDataSource`/`SunbeamDataSource`]_


```toml
[ingress_data_source]
data_source_type = "InfluxDBDataSource"
```


### `[ingress_data_source.InfluxDBDataSource]`

Options for `InfluxDBDataSource`.

1. `start`: Required. A string containing the beginning of relevant time-series data as an ISO8061 formatted string.
2. `stop`: Required. A string containing the end of relevant time-series data as an ISO8061 formatted string.
3. `url`: Required. A string containing the URL to the InfluxDB API that should be used to fetch data for ingress into this pipeline.
```toml
[ingress_data_source.InfluxDBDataSource]
start = "2024-07-01T01:00:00Z"
stop = "2024-08-30T01:00:00Z"
url = "http://influxdb.telemetry.ubcsolar.com"
```


### `[ingress_data_source.MongoDBDataSource]`

Options for `MongoDBDataSource` when used as the ingress data source.

1. `ingress_origin`: Required. A string containing the name of the pipeline from which data should be fetched from for ingress into the current pipeline. 

```toml
[ingress_data_source.MongoDBDataSource]
ingress_origin = "influxdb_cache"
```

### `[ingress_data_source.SunbeamDataSource]`

Options for `SunbeamDataSource` when used as the ingress data source.

1. `api_url`: A string containing the URL to the Sunbeam API which is to be used to fetch data for ingress into this pipeline.

```toml
[ingress_data_source.SunbeamDataSource]
api_url = "api.sunbeam.ubcsolar.com"
```


### `[ingress_data_source.FSDataSource]`

See [FSDataSource for stage data](#stage_data_sourcefsdatasource).

