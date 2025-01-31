from __future__ import annotations

from typing import TypeVar

import pydantic


def write_model_to_file(model: pydantic.BaseModel, output_file: str) -> None:
    json = model.model_dump_json(indent=2)
    with open(output_file, "w") as output:
        output.write(json)


_T = TypeVar("_T", bound=pydantic.BaseModel)


def read_model_from_file(model_class: type[_T], input_file: str) -> _T:
    with open(input_file, "r") as file:
        data = file.read()
        return model_class.model_validate_json(data)
