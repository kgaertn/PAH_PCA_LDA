from dataclasses import dataclass

@dataclass
class PC_Scores:
    """Represents a principal component score for a sample."""
    id: int
    pc_id: int
    sample_id: int
    pc_score: float