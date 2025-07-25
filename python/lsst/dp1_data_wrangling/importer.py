from __future__ import annotations

from itertools import groupby

from lsst.daf.butler import (
    Butler,
    CollectionType,
    DatasetAssociation,
    DatasetRef,
    DatasetType,
)

from .dataset_types import import_dataset_types
from .datasets_parquet import (
    read_dataset_associations_from_file,
    read_dataset_refs_from_file,
)
from .datastore_mapping import DatastoreMapper, DatastoreMappingFunction
from .datastore_parquet import read_datastore_records_from_file
from .dimension_record_parquet import read_dimension_records_from_file
from .index import ExportIndex
from .paths import ExportPaths
from .utils import read_model_from_file


class Importer:
    def __init__(self, input_path: str, butler: Butler) -> None:
        self._paths = ExportPaths(input_path)
        self._butler = butler

    def import_all(self, datastore_mapping: DatastoreMappingFunction) -> ExportIndex:
        index = read_model_from_file(ExportIndex, self._paths.index_path())

        # Dataset types have to be registered outside the transaction,
        # because registering them creates tables.
        dataset_types = import_dataset_types(self._paths.dataset_type_path(), self._butler.dimensions)
        for dt in dataset_types:
            self._butler.registry.registerDatasetType(dt)

        with self._butler.transaction():
            self._butler.import_(filename=self._paths.collections_path())
            self._import_dimension_records(index.dimensions)
            self._import_datasets(dataset_types)
            self._import_associations(dataset_types)
            self._import_datastore(datastore_mapping)

        return index

    def _import_dimension_records(self, dimensions: list[str]) -> None:
        universe = self._butler.dimensions
        dimensions = universe.sorted(dimensions)
        for dimension_name in dimensions:
            element = universe[dimension_name]
            # If a dimension doesn't "have its own table", then it's a virtual
            # dimension defined by another dimension, and we can't insert rows
            # for it.  In the default LSST universe, "band" doesn't have its
            # own table because it is derived from "physical_filter".
            if element.has_own_table:
                path = self._paths.dimension_parquet_path(element.name)
                for table in read_dimension_records_from_file(element, path):
                    self._butler.registry.insertDimensionData(element, *list(table), skip_existing=True)

    def _import_datasets(self, dataset_types: list[DatasetType]) -> None:
        for dt in dataset_types:
            path = self._paths.dataset_parquet_path(dt.name)
            for batch in read_dataset_refs_from_file(dt, path):
                # _importDatasets can only import refs from one run at a time,
                # so chunk by run.
                for run, refs in groupby(sorted(batch, key=_get_run), _get_run):
                    self._butler.registry._importDatasets(
                        refs,
                        # Setting expand=False will break things if "live
                        # ObsCore" is enabled, but expand=True is unacceptably
                        # slow because it generates a query for every single
                        # ref we are inserting.  We currently do not plan to
                        # use live ObsCore for data releases.
                        #
                        # TODO: The export data contains the dimension records
                        # necessary to expand the data IDs ourselves, so
                        # writing the logic to plumb that in would allow Live
                        # ObsCore to work without the queries.
                        expand=False,
                    )

    def _import_associations(self, dataset_types: list[DatasetType]) -> None:
        for dt in dataset_types:
            path = self._paths.dataset_association_parquet_path(dt.name)
            for batch in read_dataset_associations_from_file(dt, path):
                batch.sort(key=_get_collection)
                for collection, rows in groupby(batch, _get_collection):
                    collection_type = self._butler.collections.get_info(collection).type
                    if collection_type == CollectionType.TAGGED:
                        self._butler.registry.associate(collection, [r.ref for r in rows])
                    elif collection_type == CollectionType.CALIBRATION:
                        for row in rows:
                            self._butler.registry.certify(collection, [row.ref], row.timespan)
                    else:
                        raise ValueError(
                            f"Unexpected collection type '{collection_type}'"
                            f" when importing associations for dataset type '{dt.name}'"
                        )

    def _import_datastore(self, datastore_mapping: DatastoreMappingFunction) -> None:
        mapper = DatastoreMapper(datastore_mapping, self._butler._datastore)
        for batch in read_datastore_records_from_file(self._paths.datastore_parquet_path()):
            records = mapper.map_to_target(batch)
            self._butler._datastore.import_records(records)


def _get_run(ref: DatasetRef) -> str:
    return ref.run


def _get_collection(association: DatasetAssociation) -> str:
    return association.collection
