import pydantic
from lsst.daf.butler import DatasetType, SerializedDatasetType


def export_dataset_types(output_file: str, dataset_types: list[DatasetType]) -> None:
    serialized = [dt.to_simple() for dt in dataset_types]
    json = DatasetTypeExport(dataset_types=serialized).model_dump_json(indent=2)
    with open(output_file, "w") as output:
        output.write(json)


class DatasetTypeExport(pydantic.BaseModel):
    dataset_types: list[SerializedDatasetType]
