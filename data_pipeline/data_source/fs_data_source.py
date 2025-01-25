import os

from data_tools import File, FileType
from data_tools.schema import DataSource, Result, FileLoader, CanonicalPath
from pathlib import Path
import dill


class FSDataSource(DataSource):
    def __init__(self, data_source_config: dict):
        super().__init__()
        self._root = Path(data_source_config["root"]).absolute()

    def canonical_path_to_real_path(self, canonical_path: CanonicalPath):
        return str(self._root / canonical_path.to_path()) + ".bin"

    def store(self, file: File) -> FileLoader:
        match file.file_type:
            case FileType.TimeSeries:
                if file.data is not None:
                    path = self.canonical_path_to_real_path(file.canonical_path)
                    os.makedirs(Path(path).parent, exist_ok=True)

                    with open(self.canonical_path_to_real_path(file.canonical_path), "wb") as f:
                        dill.dump(file.data, f)

                return FileLoader(lambda x: self.get(x), file.canonical_path)

            case _:
                raise RuntimeError(f"FSDataSource does not support the storing of {file.file_type}!")

    def get(self, canonical_path: CanonicalPath, **kwargs) -> Result:
        try:
            with open(self.canonical_path_to_real_path(canonical_path), "rb") as f:
                return Result.Ok(dill.load(f))

        except FileNotFoundError as e:
            return Result.Err(e)
