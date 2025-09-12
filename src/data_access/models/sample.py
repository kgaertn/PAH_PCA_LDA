from dataclasses import dataclass

@dataclass
class Sample:
    """Represents a measurement sample with bow stroke boundaries."""
    id: int 
    measurement_id: int
    bow_stroke_start: int
    bow_stroke_end: int