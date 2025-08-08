from dataclasses import dataclass
from typing import Optional

@dataclass
class Experiment:
    """Represents an experiment with its data state and storage info."""
    id: int | str
    name: str
    data_state: str
    data_folder: Optional[str] = None
    upload_complete: Optional[int] = None
    