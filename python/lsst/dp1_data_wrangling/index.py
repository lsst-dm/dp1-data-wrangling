from __future__ import annotations

import pydantic


class ExportIndex(pydantic.BaseModel):
    dimensions: list[str]
    dataset_types: list[str]
