import pydantic
from lsst.daf.butler import DatasetType, DimensionUniverse, SerializedDatasetType

from .utils import read_model_from_file, write_model_to_file


def export_dataset_types(output_file: str, dataset_types: list[DatasetType]) -> None:
    serialized = [dt.to_simple() for dt in dataset_types]
    model = DatasetTypeExport(dataset_types=serialized)
    write_model_to_file(model, output_file)


def import_dataset_types(input_file: str, universe: DimensionUniverse) -> list[DatasetType]:
    model = read_model_from_file(DatasetTypeExport, input_file)
    return [DatasetType.from_simple(dt, universe=universe) for dt in model.dataset_types]


class DatasetTypeExport(pydantic.BaseModel):
    dataset_types: list[SerializedDatasetType]
