from __future__ import annotations

from typing import TypeVar

import pydantic
from lsst.daf.butler import DatasetId


def write_model_to_file(model: pydantic.BaseModel, output_file: str) -> None:
    json = model.model_dump_json(indent=2)
    with open(output_file, "w") as output:
        output.write(json)


_T = TypeVar("_T", bound=pydantic.BaseModel)


def read_model_from_file(model_class: type[_T], input_file: str) -> _T:
    with open(input_file, "r") as file:
        data = file.read()
        return model_class.model_validate_json(data)


def convert_parquet_uuid_to_dataset_id(dataset_id_binary: object) -> DatasetId:
    assert isinstance(dataset_id_binary, bytes), "Dataset ID expected to be serialized as binary bytes."
    return DatasetId(bytes=dataset_id_binary)
