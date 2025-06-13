from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Experiment:
    id: int
    name: str
    data_state: str
    data_folder: Optional[str] = None
    upload_complete: Optional[int] = None
    