from dataclasses import dataclass

@dataclass
class Datapoint:
    id: int
    measurement_id: int
    bow_stroke: int
    up_down: int
    key:str
    time_point: int
    value: float