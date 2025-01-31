from __future__ import annotations

from lsst.daf.butler import Butler

from .dataset_types import import_dataset_types
from .index import ExportIndex
from .paths import ExportPaths
from .utils import read_model_from_file

INPUT_DIRECTORY = "dp1-dump-test"
OUTPUT_REPO = "import-test-repo"


def main() -> None:
    Butler.makeRepo(OUTPUT_REPO)
    butler = Butler(OUTPUT_REPO, writeable=True)
    importer = Importer(INPUT_DIRECTORY, butler)
    importer.import_all()


class Importer:
    def __init__(self, input_path: str, butler: Butler) -> None:
        self._paths = ExportPaths(input_path)
        self._butler = butler

    def import_all(self) -> None:
        index = read_model_from_file(ExportIndex, self._paths.index_path())

        # Dataset types have to be registered outside the transaction,
        # because registering them creates tables.
        dataset_types = import_dataset_types(self._paths.dataset_type_path(), self._butler.dimensions)
        for dt in dataset_types:
            self._butler.registry.registerDatasetType(dt)

        with self._butler.transaction():
            self._butler.import_(filename=self._paths.collections_path())
