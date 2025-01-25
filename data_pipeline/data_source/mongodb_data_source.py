from data_tools.schema import DataSource, FileLoader, File, Result, CanonicalPath, FileType
import pymongo
import logging
import dill


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()


class MongoDBDataSource(DataSource):
    def __init__(self):
        super().__init__()

        def init_db():
            self._metadata_collection.insert_one({
                "type": "status"
            })

            self._metadata_collection.insert_one({
                "type": "commissioned_pipelines",
                "data": []
            })

        self._client = pymongo.MongoClient("mongodb://mongodb:27017/")

        self._db = self._client.sunbeam_db

        self._metadata_collection = self._db.metadata
        self._time_series_collection = self._db.time_series_data
        db_status = self._metadata_collection.find_one({"type": "status"})

        if db_status is None:
            logger.info("MongoDB is not initialized. Initializing...")
            init_db()

        self._time_series_collection.create_index([("origin", 1), ("source", 1),
                                                   ("event", 1), ("name", 1),
                                                   ("path", 1)], unique=True)

        logger.info("Connection to MongoDB is initialized!")

    def store(self, file: File) -> FileLoader:
        match file.file_type:
            case FileType.TimeSeries:
                if file.data is not None:
                    serialized_object = dill.dumps(file.data)

                    self._time_series_collection.insert_one(
                        {
                            "origin": file.canonical_path.origin,
                            "source": file.canonical_path.source,
                            "event": file.canonical_path.path[0],
                            "path": file.canonical_path.path[1:],
                            "name": file.canonical_path.name,
                            "data": serialized_object
                        }
                    )

                return FileLoader(lambda x: self.get(x), file.canonical_path)

            case _:
                raise RuntimeError(f"FSDataSource does not support the storing of {file.file_type}!")

    def get(self, canonical_path: CanonicalPath, **kwargs) -> Result:
        try:
            result = self._time_series_collection.find_one({
                "origin": canonical_path.origin,
                "source": canonical_path.source,
                "event": canonical_path.path[0],
                "path": canonical_path.path[1:],
                "name": canonical_path.name,
            })

            if not result:
                raise RuntimeError(f"Could not find file at {canonical_path.to_string()}!")
            serialized_data = result.get("data")
            data = dill.loads(serialized_data)

            return Result.Ok(data)

        except Exception as e:
            return Result.Err(e)
