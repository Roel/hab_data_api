from dataclasses import dataclass


@dataclass
class HeatPumpStatusDto:
    operating_mode: str
    heat_source: str
    defrost_status: str


@dataclass
class HeatPumpSetpointDto:
    dhw: float
    heating: float
