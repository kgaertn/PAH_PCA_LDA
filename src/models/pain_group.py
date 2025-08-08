from dataclasses import dataclass

@dataclass
class PainGroup:
    """Represents a pain group category."""
    id: int
    pain_group: str