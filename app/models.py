from pydantic import BaseModel
from typing import Optional

class IngestPayload(BaseModel):
    group1: dict   # {"suhu": float, "kelembapan": float}
    group2: dict   # {"lux": float}
    timestamp: Optional[int] = None
