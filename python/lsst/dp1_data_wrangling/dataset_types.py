import pydantic
from lsst.daf.butler import DatasetType, DimensionUniverse, SerializedDatasetType


def export_dataset_types(output_file: str, dataset_types: list[DatasetType]) -> None:
    serialized = [dt.to_simple() for dt in dataset_types]
    json = DatasetTypeExport(dataset_types=serialized).model_dump_json(indent=2)
    with open(output_file, "w") as output:
        output.write(json)


def import_dataset_types(input_file: str, universe: DimensionUniverse) -> list[DatasetType]:
    with open(input_file, "r") as file:
        data = file.read()
        model = DatasetTypeExport.model_validate_json(data)
        return [DatasetType.from_simple(dt, universe=universe) for dt in model.dataset_types]


class DatasetTypeExport(pydantic.BaseModel):
    dataset_types: list[SerializedDatasetType]
