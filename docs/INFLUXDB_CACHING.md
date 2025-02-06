# InfluxDB Caching

How can we cache InfluxDB? This is a feature available to the `MongoDBDataSource`.

First of all, `sunbeam.toml` should have the `ingress_data_source` set to `data_source_type = "InfluxDBDataSource"`, of course.

Next, set `stage_data_source`'s `data_source_type = "MongoDBDataSource"`, and set `stages_to_run` to `ingress` only.

You'll want this configuration on a different branch, let's say its called `influxdb_cache`. 

Now, deploy the pipeline `influxdb_cache` and watch it process all the time-series data from InfluxDB.

Now, here's the fun part. In other pipelines, not the InfluxDB cache pipeline, set the `ingress_origin` flag of the `MongoDBDataSource` to `influxdb_cache`. Since all pipelines share the same MongoDB database, we can point all other pipelines to grab their ingress data from the `influxdb_cache`!

Remember, regular stages still only can access the data available in the pipeline's own warehouse of data: only the ingress stage when using `MongoDBDataSource` for ingress gets to be special and look into other pipeline warehouses. So, the ingress stage of pipelines with their `ingress_origin` pointed to `influxdb_cache` will copy over the data into the pipeline's own warehouse.  