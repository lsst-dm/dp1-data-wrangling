import pathlib
import re

_DIMENSION_SUBDIRECTORY = "dimensions"
_DATASETS_SUBDIRECTORY = "datasets"
_ASSOCIATION_SUBDIRECTORY = "associations"


class ExportPaths:
    def __init__(self, output_directory: str) -> None:
        self._dir = pathlib.Path(output_directory)

    def create_directories(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        for dir in [_DIMENSION_SUBDIRECTORY, _DATASETS_SUBDIRECTORY, _ASSOCIATION_SUBDIRECTORY]:
            self._dir.joinpath(dir).mkdir(exist_ok=True)

    def _join(self, *path_fragments: str) -> str:
        # Make sure a poisoned filename can't escape the export directory.
        allowed_fragment_name_regex = r"^\w+\.?\w*$"
        for fragment in path_fragments:
            if not re.fullmatch(allowed_fragment_name_regex, fragment):
                raise RuntimeError(f"Path segment is in unexpected format: {fragment}")

        return str(self._dir.joinpath(*path_fragments))

    def dimension_parquet_path(self, dimension_name: str) -> str:
        return self._join(_DIMENSION_SUBDIRECTORY, dimension_name)

    def dataset_parquet_path(self, dataset_type_name: str) -> str:
        return self._join(_DATASETS_SUBDIRECTORY, dataset_type_name)

    def dataset_association_parquet_path(self, dataset_type_name: str) -> str:
        return self._join(_ASSOCIATION_SUBDIRECTORY, dataset_type_name)

    def datastore_parquet_path(self) -> str:
        return self._join("datastore")

    def collections_path(self) -> str:
        return self._join("collections.yaml")

    def dataset_type_path(self) -> str:
        return self._join("dataset_types.json")

    def index_path(self) -> str:
        return self._join("index.json")
