# {
#   "user_id": 1,
#   "timestamp": 12312412312,
#   "value": {
#     "acc": {
#       "hz": 32,
#       "value": [
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1},
#         {"x": 0.1, "y": 0.1, "z": 0.1}
#       ]
#     },
#     "bvp": {
#       "hz": 64,
#       "value": [
#         0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
#         0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
#         0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
#         0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
#         0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
#         0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
#         0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
#         0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1
#       ]
#     },
#     "eda": {
#       "hz": 4,
#       "value": [0.1, 0.1, 0.1, 0.1]
#     },
#     "hr": {
#       "hz": 1,
#       "value": [0.1]
#     }
#   }
# }

from typing import TypedDict, List


class Axis(TypedDict):
    x: float
    y: float
    z: float


class Accelerometer(TypedDict):
    hz: int
    value: List[Axis]


class BloodVolumePulse(TypedDict):
    hz: int
    value: List[float]


class Electrodemal(TypedDict):
    hz: int
    value: List[float]


class HeartRate(TypedDict):
    hz: int
    value: List[float]


class Temperature(TypedDict):
    hz: int
    value: List[float]


class SensorValue(TypedDict):
    acc: Accelerometer
    bvp: BloodVolumePulse
    eda: Electrodemal
    hr: HeartRate
    temp: Temperature


class SensorRecord(TypedDict):
    user_id: str
    timestamp: int
    value: SensorValue
