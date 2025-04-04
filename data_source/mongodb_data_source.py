from data_tools.schema import DataSource, FileLoader, File, Result, CanonicalPath, FileType
import pymongo
import logging
import dill


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()


class MongoDBDataSource(DataSource):
    def __init__(self, *args, **kwargs):
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
                                                   ("event", 1), ("name", 1)], unique=True)

        logger.info("Connection to MongoDB is initialized!")

    def store(self, file: File) -> FileLoader:
        match file.file_type:
            case FileType.TimeSeries:
                if file.data is not None:
                    serialized_object = dill.dumps(file.data)

                    self._time_series_collection.replace_one(
                        filter={
                            "origin": file.canonical_path.origin,
                            "source": file.canonical_path.source,
                            "event": file.canonical_path.event,
                            "name": file.canonical_path.name
                        },
                        replacement={
                            "origin": file.canonical_path.origin,
                            "source": file.canonical_path.source,
                            "event": file.canonical_path.event,
                            "name": file.canonical_path.name,
                            "data": serialized_object,
                            "metadata": file.metadata if file.metadata is not None else {},
                            "description": file.description if file.description is not None else "",
                            "filetype": str(file.file_type)
                        },
                        upsert=True  # Insert if it doesn't exist, otherwise replace
                    )

                return FileLoader(lambda x: self.get(x), file.canonical_path)

            case _:
                raise RuntimeError(f"MongoDBDataSource does not support the storing of {file.file_type}!")

    def get(self, canonical_path: CanonicalPath, **kwargs) -> Result:
        try:
            result = self._time_series_collection.find_one({
                "origin": canonical_path.origin,
                "source": canonical_path.source,
                "event": canonical_path.event,
                "name": canonical_path.name,
            })

            if not result:
                raise RuntimeError(f"Could not find file at {canonical_path.to_string()}!")

            serialized_data = result.get("data")  # The data is stored as serialized bytes!

            return Result.Ok(
                File(
                    canonical_path=canonical_path,
                    file_type=result.get("filetype"),
                    metadata=result.get("metadata"),
                    description=result.get("description"),
                    data=dill.loads(serialized_data),
                )
            )

        except Exception as e:
            return Result.Err(e)
