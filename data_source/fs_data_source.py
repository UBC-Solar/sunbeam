import os

from data_tools import File, FileType
from data_tools.schema import DataSource, Result, FileLoader, CanonicalPath
from pathlib import Path
import dill
from config import FSDataSourceConfig


class FSDataSource(DataSource):
    def __init__(self, data_source_config: FSDataSourceConfig):
        super().__init__()
        self._root = (Path(__file__).parent.parent / data_source_config.fs_root).absolute()

    def canonical_path_to_real_path(self, canonical_path: CanonicalPath):
        return str(self._root / canonical_path.to_path()) + ".bin"

    def store(self, file: File) -> FileLoader:
        if file.data is not None:
            path = self.canonical_path_to_real_path(file.canonical_path)
            os.makedirs(Path(path).parent, exist_ok=True)

            with open(self.canonical_path_to_real_path(file.canonical_path), "wb") as f:
                dill.dump(file, f, protocol=dill.HIGHEST_PROTOCOL)

        return FileLoader(lambda x: self.get(x), file.canonical_path)

    def get(self, canonical_path: CanonicalPath, **kwargs) -> Result:
        try:
            with open(self.canonical_path_to_real_path(canonical_path), "rb") as f:
                return Result.Ok(dill.load(f))

        except FileNotFoundError as e:
            return Result.Err(e)
