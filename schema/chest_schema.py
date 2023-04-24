"""
{
  "user_id": 1,
  "timestamp": 12312412312,
  "window_size": 2,
  "value": [
    {
      "chest_acc": {
        "hz": 700,
        "value": [...]
      },
      "chest_ecg": {
        "hz": 700,
        "value": [...]
      },
      "chest_eda": {
        "hz": 700,
        "value": [...]
      },
      "chest_emg": {
        "hz": 700,
        "value": [...]
      },
      "chest_temp": {
        "hz": 700,
        "value": [...]
      },
      "chest_resp": {
        "hz": 700,
        "value": [...]
      }
    },
    {
      "chest_acc": {
        "hz": 700,
        "value": [...]
      },
      "chest_ecg": {
        "hz": 700,
        "value": [...]
      },
      "chest_eda": {
        "hz": 700,
        "value": [...]
      },
      "chest_emg": {
        "hz": 700,
        "value": [...]
      },
      "chest_temp": {
        "hz": 700,
        "value": [...]
      },
      "chest_resp": {
        "hz": 700,
        "value": [...]
      }
    }
  ]
}
"""

from typing import TypedDict, List


class ChestAxis(TypedDict):
    x: int
    y: int
    z: int


# Chest Accelerometer
class ChestACC(TypedDict):
    hz: int
    value: List[ChestAxis]


# Chest Electrocardiogram
class ChestEGC(TypedDict):
    hz: int
    value: List[int]


# Chest Electrodemal
class ChestEDA(TypedDict):
    hz: int
    value: List[int]


# Chest Electromyogram
class ChestEMG(TypedDict):
    hz: int
    value: List[int]


# Chest Temperature
class ChestTemp(TypedDict):
    hz: int
    value: List[int]


# Chest Respiration
class ChestResp(TypedDict):
    hz: int
    value: List[int]


class ChestDeviceSensorValue(TypedDict):
    chest_acc: ChestACC
    chest_ecg: ChestEGC
    chest_eda: ChestEDA
    chest_emg: ChestEMG
    chest_temp: ChestTemp
    chest_resp: ChestResp


class ChestDeviceSensorRecord(TypedDict):
    user_id: str
    timestamp: int
    window_size: int
    value: List[ChestDeviceSensorValue]
