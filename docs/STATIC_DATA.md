# Static Data

When a stage is instantiated, it will look for a folder of its own name in the directory that the `.py` source file is in. `ingest` stage in `stage/ingress.py` will look for a directory `stage/ingress`. Then, the stage will traverse the directory to build a dictionary of static stage data. 
For example, a file structure looking like,
```
└── ingress
    ├── data1.json
    └── more_data
        ├── data2.json
        ├── data3.toml
        └── even_more_data
            └── data4.toml
```
will result in a dictionary like,
```python
{
    "data1": ...,
    "more_data": {
        "data2": ...,
        "data3": ...,
        "even_more_data": {
            "data4": ...
       }
    }
 }
```
which is accessible through the `stage_data` property of a `Stage`.

Files of type `json`, `npy` (Numpy File, not `npz` NumPy Archive!), `pickle/pkl` (Python Pickle File), `toml`, and `csv` are supported and will be automatically parsed into their respective contents.

A keyword argument `data_pattern` may, but is not mandated, be provided to the call of a stage's superclass `__init__()` call to provide a predicate that will exclude certain directories from being loaded. For example, maybe a stage only wants to load data specifically for the event it is processing. So, you may want to set `data_pattern = lambda dir_name: dir_name == event.name` to exclude all directories that don't match the event name. 