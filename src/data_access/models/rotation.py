from dataclasses import dataclass

@dataclass
class RotationPCA:
    """Represents PCA rotation metadata."""
    id: int
    rotation_type: str