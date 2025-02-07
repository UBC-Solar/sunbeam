# Introduction to Sunbeam

## Preface

Sunbeam is UBC Solar's real-time and post-mortem data pipeline. Simple, right? Not exactly. The purpose of this guide is to document Sunbeam and act as an introduction to how contribute to its non-trivial workings and internal paradigms. 

Sunbeam was designed to both be heavily object-oriented (which is the ideal way to use Python) while also taking inspiration from functional programming (which lends itself to data transformation workflows). In Sunbeam, everything is an object. Configuration is wrapped in an object, results are objects, processing stages are objects, errors are objects. However, as functional programming requires: all of these objects are stateless—processing stages are memoryless systems. As you’ll see, the combination of these principles lends itself well to the design of a data pipeline.

I’d like to introduce you to Sunbeam one element at a time, explaining not just what things are but also why they are how they are, so hopefully you get a sense of how the system works together.

## Schema

I’m going to actually start at the bottom: let’s introduce a few critical actors of this Sunbeam's schema: how data is represented and the objects working with it.

### `Result` 
The `Result` type is an algebraic data type that can be thought to be a type that can actually be one of two types: a result, or an error. Most things in Sunbeam return a `Result` type. Instead of raising exceptions, the intention is to wrap them and handle them when is convenient, rather than breaking execution flow. Additionally, it’s easy to tell when a processing step failed to produce a valid result. A `Result`  can be unwrapped to reveal the data, or error contained within, when convenient. The truth value of a `Result` is `True` when it contains data, and `False` when it contains an error.

### `File` 
The `File` type can be thought of as the “atomic unit of data” of Sunbeam: the smallest piece of data that Sunbeam can store, produce, and return. A `File` encapsulates a single item of “data”, not as in an array element but as in “the array power as a function of time during FSGP 2024, Day 1”. Sunbeam handles data in a file-system like way, but that is simultaneously agnostic to whether it’s storing data in a database or actual file system (more on this later!). Consequently, a `File` may equally be concretely existing on a hard drive or database. A `File` is a container: it has a `CanonicalPath`, which represents their location in the system they’re stored in (again, this might be a database or an actual file system), a `FileType`, which declares what kind of data it contains, the actual data, and also optionally a description (a `str`) and any additional optional metadata (a `dict`). A `File` can sotre any object: arrays of data, single values, or anything in between; this representation agnostic way of encapsulating data enhances the extensibility of Sunbeam to handle all sorts of data on all sorts of systems and solutions (ex. develop and test locally and store data in your file system then deploy to a server and store data in a database—Sunbeam ensures that you can expect the same behaviour no matter how a `File` actually is stored).

### `FileLoader`
The `FileLoader` type is a type that can be thought to map to a `File` (actually, a `Result`, since maybe the `File` cannot be found). That is, a `FileLoader` is a callable object which wraps code that can be called to try to acquire a `File` that it points to (again, this could be a `File` in a file system or in a database, and the `FileLoader` will handle the process of acquiring the `File` from wherever it was stored). A `FileLoader` are usually returned from processing stages so that the results can be obtained whenever they are desired. 
Why return a `FileLoader` instead of just the actual `File`? To ensure scalability with memory for large processing pipelines, and in the potential case of parallelizing Sunbeam in the future. 

>Sunbeam was NOT designed to maximize CPU performance, but memory performance could be a problem with even our level of data, so having data not all be stored in memory concurrently while processing stages are running is a precaution.

### `DataSource`

A `DataSource` is the magic that lets this "equally use a local filesystem to store data, or a database" actually happen. 
The `DataSource` type is actually an abstract type, and must be implemented. A DataSource represents an interface to some storage of data, whether that be a filesystem or a database. The implementation must satisfy the same interface, which isolates the functionality of Sunbeam from the details of how it is storing and loading data. 
Currently, DataSource is implemented for local file systems (`FSDataSource`) and InfluxDB (`InfluxDBDataSource`), and MongoDB (`MongoDBDataSource`). 

>Convince yourself at this point that Sunbeam is designed to equally use a local filesystem to store data, or a database, without changes to its actual function. This is a critical idea.

### `Context`

The `Context`type is a singleton. That is, only one instance of the class exists *globally*, and this instance can be acquired at whim, anywhere. If regular classes were text messages being passed around, a singleton is a message being broadcast on a public radio channel (anyone can get access to it, and its the same for everyone).
The `Context` stores important information about the data pipeline that all processing stages need to be able to access freely. For example, `Context` stores a reference to the instance of the `DataSource` that stages should use to store and get data.

## `Stage`

Now that you know some of the important types that Sunbeam uses, let's talk about processing stages (henceforth just a “stage”). There’s no specific requirement for how big or small a stage needs to be, that’s up to you. However, all stages follow a similar structure. I don't think it would be the best use of your time to read paragraphs of theory about what a stage does and how to build one, so I think a better way would be to show you. Let's build a (simple) stage together.

Firstly, a stage is written as a class inheriting from the Stage base class, which forces subclasses to implement a set of interfaces that define a correct stage.
```python
class PowerStage(Stage):
	"""
	This Stage combines TotalPackVoltage and PackCurrent to calculate PackPower
	"""
```

All stages have a canonical name that uniquely identifies that stage.
```python
    @classmethod  
    def get_stage_name(cls):  
        return "power"
```

All stages must declare what other stages they depend on.
```python
    @staticmethod  
    def dependencies():  
        return ["ingress"]
```
Here, we declare that we need the `ingress` stage before running `power` , notice that while the list is a single element in this case, in general you can have stages can rely on multiple input stages.

Now, we can define how to create an instance of our `PowerStage` (remember, all things are objects in Sunbeam!)
```python
    def __init__(self, event_name: str):  
        """  
        :param str event_name: which event is currently being processed    
        """    
        super().__init__()  
      
        self._event_name = event_name  
      
        self.declare_output("pack_power")  
```
We do a few things here. Firstly, we initialize the ``Stage`` base class. Next, we save the name of the event that we are currently processing, such as "FSGP Day 1", so that we know how to properly save our outputs to the right spot. Lastly, we declare what outputs we are intending to produce. 

> When I said that stages are stateless, I meant it! To force this statelessness, trying to set an attribute after `__init__` will raise an error.

Sunbeam's stages follow the ETL workflow: extract, transform, load. Let's implement the `extract` method.
```python
    def extract(self, total_pack_voltage_loader: FileLoader, pack_current_loader: FileLoader) -> tuple[Result, Result]:  
        total_pack_voltage_result: Result = total_pack_voltage_loader()  
        pack_current_result: Result = pack_current_loader()  
      
        return total_pack_voltage_result, pack_current_result
```
As inputs, we take in `FileLoader`s to `TotalPackVoltage` and `PackCurrent`, which come from the `ingress` stage. All we need to do in `extract` is actually invoke the `FileLoader` callables, which produces  a corresponding pair of`Result` types.

> Please note how every parameter and variable is type-hinted, as well as the function return signature! Make sure your stages abide by this practice, too.

Next, we can implement `transform`. Notably, the inputs to `transform` will be the outputs of `extract`.
```python
def transform(self, total_pack_voltage_result: Result, pack_current_result: Result) -> tuple[Result]:    
    try:  
        total_pack_voltage: TimeSeries = total_pack_voltage_result.unwrap()  
        pack_current: TimeSeries = pack_current_result.unwrap()  
  
        total_pack_voltage, pack_current = TimeSeries.align(total_pack_voltage, pack_current)  
        pack_power = total_pack_voltage.promote(total_pack_voltage * pack_current)  
        pack_power.units = "W"  
        pack_power.name = "Pack Power"  
  
        pack_power = Result.Ok(pack_power)  
  
    except UnwrappedError as e:  
        self.logger.error(f"Failed to unwrap result! \n {e}")  
        pack_power = Result.Err(RuntimeError("Failed to process pack power!"))  
  
    return pack_power  
```
As you can see, we take in the pair of `Result` produced by `extract`, and then unwrap them in a `try - except` block.
If neither `FileLoader` fails, we align the time axes before multiplying them to get `PackPower`, then set the metadata on the new `TimeSeries`. Finally, we wrap the new `TimeSeries` in a `Result`.

> In the case that either `FileLoader` fails, can you see what kind of error is produced when `unwrap()` contains an error? Notice that we log the error, then wrap a new error result of `transform`!

Finally, we can implement `load`.
```python
def load(self, pack_power: Result) -> tuple[FileLoader]:  
    pack_power_file = File(  
        canonical_path=CanonicalPath(  
            origin=self.context.title,  
            event=self.event_name,  
            source=PowerStage.get_stage_name(),  
            name="PackPower",  
        ),        
        file_type=FileType.TimeSeries,  
        data=pack_power.unwrap() if pack_power else None  
    )  
  
    pack_power_loader = self.context.data_source.store(pack_power_file)  
    self.logger.info(f"Successfully loaded PackPower!")  
  
    return pack_power_loader
```
Here, we first create a `File` by creating a `CanonicalPath`. The `CanonicalPath` object contains four pieces of data,

1. `origin`: the name of the pipeline, this is going to be a GitHub tag or branch name (more on this later!)
2. `event`: the event that is currently being processed (why we saved it earlier).
3. `source`: the name of the stage that is producing the data this `File` will contain.
4. `name`: the name of the data.

The `File` creation also requires the type, which we pass as `FileType.TimeSeries` (`FileType` is an `enum` that contains the kinds of data types that the `File` API supports). Finally, we pass in the data of `PackPower` if it was created successfully. 

> Remember, the truth value of a `Result` is `True` if it contains data, or `False` if it contains an error. Can you see where we use that to our advantage above?

If we didn't create `PackPower` successfully, we pass in `None` as the data. A `DataSource` does not store an empty file, but rather is supposed to interpret this as "do not actually store anything": trying to store an empty file is a "no-op" operation. Finally, we use the `DataSource` that `Context` has for us to store the `File`, which returns a `FileLoader` to the `File` we just stored! 
This `FileLoader` is the final product of the stage, and will be returned to the rest of the pipeline. 

> Now think: what will the `FileLoader` do when invoked later on if `PackPower` wasn't created and we passed `None` to the `File` so no `File` actually got stored? 
> 
> Answer: it will return a `Result` containing a `FileNotFound` error, (since it won't be able to find a `File` at the path, since `DataSource` wouldn't have created one!). This allows us to always return the same data types from a stage (always the same number of `FileLoader`s)  even if they will behave differently down the line.

That's pretty much it!

Lastly,
```python
    @staticmethod  
    @task(name="Power")  
    def run(self, total_pack_voltage_loader: FileLoader, pack_current_loader: FileLoader) -> StageResult:  
        """  
        Run the power stage, converting voltage and current data into power. 
         
        :param self: an instance of PowerStage to be run    
        :param FileLoader total_pack_voltage_loader: loader to TotalPackVoltage from Ingress    
        :param FileLoader pack_current_loader: loader to PackCurrent from Ingress 
        :returns: PackPower (TimeSeries)
        """    
        return super().run(self, total_pack_voltage_loader, pack_current_loader)
```
The `run` function is what is actually what we invoke to run a stage. We always should just subclass it trivially (it just calls the base class implementation) to set the parameters and documentation, as well as add the `@task(name="Power)` decorator (more on this later!). 

> See that `run` is a static method, yet takes in `self`! What do you think this looks like when calling it? `run` needs to be static so it doesn't interfere with the aforementioned `task` decorator, so we need to manually pass in an instance of `PowerStage` to be `self.

How do stages get called in Sunbeam? Let's see how we'd run our stage.

```python
... # other stuff

power_stage: PowerStage = PowerStage(event_name)  
pack_power, = PowerStage.run(  
    power_stage,  
    total_pack_voltage,  
    pack_current,  
)

... # more stuff
```

> Pop Quiz: What type is `pack_power`? No, the signature of `run` is not going to help you here. Answer in a bit.

Why all this stuff? Because Python doesn’t give us the ability to write contracts between parts of our code out of the box; and this is the price we pay to use Python. 
You’re going to get an error if you accidentally try to run a stage before its dependencies. You’re going to get an error if you forget to produce the right number of outputs. Not just NullPointerException errors, but good, descriptive errors that are going to tell you what’s wrong and how to fix it. A data pipeline is a complex piece of equipment, and so we need to put in the elbow grease that builds up these contracts between parts of our systems that allow us to write code confidently. Yes, writing a stage is tedious, but the result is that every step has a layer of guarantees that you can rely on, and subsequent stages too, and the developers who will maintain your code after you.

> Answer: a `FileLoader` of course! 

## Ingress Stage

The `ingress` stage is special. So special it gets its own section!

Most of our data comes in as raw time-series data from InfluxDB. It must be queried, subdivided into events, and formatted as `TimeSeries` before it can properly enter Sunbeam. 

This is the job of the `ingress` stage.

To accomplish this, the `ingress` stage has its very own `DataSource` (the `InfluxDBDataSource`) which it has exclusive access to, and for the purpose of getting this raw data. This means that Sunbeam doesn't actually use a single instance of `DataSource`, but **two**: one for stages, and another for ingress.

However, this is by far the slowest part of the data pipeline. But, the raw time-series data doesn't change. So, Sunbeam currently allows for the `ingress` stage to draw from an `InfluxDBDataSource` **once**, and then cache the processed time-series data in either an `FSDataSource` or `MongoDBDataSource`, and then *all other pipelines and stages in perpetuity* can used that cached data, this is described more [concretely here](INFLUXDB_CACHING.md).

The `ingress` stage outputs all of its many outputs as a dictionary instead of each output as a separate symbol to reduce the clutter (genuinely, this is the only reason).

## Configuration

Sunbeam is configured through a strongly-object-oriented configuration system where configuration elements map bijectively to configuration objects. This deserves its own section which is [here](CONFIGURATION.md). 

Notably, all configuration files in are in the `config` directory.

1. `sunbeam.toml`: all configuration relating to the execution of Sunbeam, including `DataSource` to be used for stages and ingress, stages to run, and such.
2. `events.toml`: a list of all the events that should be processed
3. `ingress.toml`: a description of all of the raw time-series targets that should be processed from InfluxDB

## Execution

There are two ways that you can run Sunbeam. You can run Sunbeam locally, or you can launch it as a containerized system with Docker Compose. Let's talk about the first method. 

### Locally

The simplest way to run Sunbeam is to run its entrypoint, `pipeline/run.py`. If you're running locally, you probably want to have the ingress and stage data source set to `FSDataSource` in `sunbeam.toml`. If its your first time running locally, you'll need the ingress data source set to `InfluxDBDataSource`, but then you can switch it to `FSDataSource` afterwards and it can use the cached data. The files that are stored locally are pickled binary files, and you can,

```python
import pickle
from data_tools import TimeSeries

with open("path/to/my/file", "rb") as my_binary_pickled_file_from_sunbeam:
	data = pickle.load(my_binary_pickled_file_from_sunbeam)

	assert isintance(data, TimeSeries)
```
The loaded data will be the data, exactly as it was stored.

### Containerized

Let’s back up a bit. To give you a bit of context, I worked at General Fusion doing data analysis and so I had a lot of exposure to their data pipeline (elements of which inspired elements of Sunbeam, and elements of which I intentionally avoided, too). 

One important feature that I saw there that I wanted Sunbeam to have was the ability for multiple “versions” of the data pipeline to coexist, seamlessly. Why? Obviously, this is important for development of new pipeline features (you can have your development version running without touching the production pipeline) but also for consistency. Maybe a new feature accidentally breaks something old? Easy! You can just have both versions of the pipeline while the issue is getting fixed (no downtime for data analysis!). 

If you haven’t realized this yet, what this requires is a buffer between the data pipeline **logic**, and the infrastructure that **runs** the data pipeline. There must be some system which manages this different versions. 

For this, Sunbeam uses a workflow management framework called **Prefect**. Prefect allows you to deploy any number of versions of the pipeline at once, will run the pipeline, track errors and successes, and has a really nice dashboard, too. Prefect “runs” the pipeline by pulling it from GitHub, rather than needing to store code locally. 

To orchestrate everything, Sunbeam uses Docker Compose. Once again, this deserves its own [section](CONTAINERS.md), but you should read the following section on Prefect, first.

### Introduction to Prefect

Prefect manages and actually runs the data pipeline. How? I'm glad you asked! There are a two pieces of the puzzle to know about,

1. A Prefect **server** instance, which is what handles all the Prefect magic. 
2. A Prefect **agent**, which is what actually runs the data pipeline when the server tells it to.

Presuming you have the above two things talking to each other, how do you run the pipeline? You tell the server to **deploy** a version of the pipeline. In fact, all you have to do is give it a Git target and GitHub repository (either a tag or branch, not a commit) so it knows where to get the code from. Then, using the Prefect UI, which the server hosts, you can schedule a deployment to run, which is called a **flow**. A flow is divided into a bunch of **tasks**, which is why we added the `@task` decorator to many functions: it lets us look at them granularly in the Prefect UI.

Where does the flow run? On the agent! The agent listens for flows which have been scheduled to run, and then pulls the code from GitHub, and runs the flow (calls `pipeline/run.py` in its environment)!

But how do **you** actually deploy a version of the pipeline? Sunbeam has its [API](API.md) for that, which handles all actual communication with Prefect and simplifies it to just a simple API call for you, the user. When a pipeline is deployed, it will be automatically ran.

> Leave interactions with Prefect to the Sunbeam API unless you know what you're doing! `MongoDBDataSource` forbids multiple `File`s of the same location from existing. So, if you manually, using the Prefect UI, try to get a pipeline to run twice, it will fail.

## Development 

I hope at this point you have an idea of how Sunbeam works. Here, I have a suggestion of how you might continue to develop Sunbeam using the containerized workflow (one that I hope you may have realized on your own),

1. Make a GitHub branch
2. Make changes, and push them 
3. Use the commission_pipeline API endpoint to deploy your branch
4. See how things went
5. Make more changes and push them
6. Use the recommission_pipeline API endpoint to realize any changes
7. Repeat steps 4-7 until complete

Of course, Sunbeam is designed to allow you to develop locally, too. 