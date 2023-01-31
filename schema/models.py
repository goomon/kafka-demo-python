class SensorData:
    """Base class interface for sensor data.

       Note:
           This is a kind of interface class for sensor data.
           You never have any chane to use this class alone.

       Attributes:
           user_id (int): User id.
           timestamp (float): Timestamp of record.
   """
    def __init__(self, user_id, timestamp):
        self.user_id = user_id
        self.timestamp = timestamp


class Accelerometer(SensorData):
    """Accelerometer(ACC) sensor data.

       Attributes:
           x (float): x-axis.
           y (float): y-axis.
           z (float): z-axis.
   """
    def __init__(self, user_id, timestamp, *args, **kwargs):
        super().__init__(user_id, timestamp)
        self.x = float(args[0])
        self.y = float(args[1])
        self.z = float(args[2])


class HeartRate(SensorData):
    """HeartRate(HR) sensor data.

        Attributes:
            value (float): sensor value.
    """
    def __init__(self, user_id, timestamp, value):
        super().__init__(user_id, timestamp)
        self.value = float(value)


class ElectrodermalActivity(SensorData):
    """Electrodermal activity(EDA) sensor data.

        Attributes:
            value (float): sensor value.
    """
    def __init__(self, user_id, timestamp, value):
        super().__init__(user_id, timestamp)
        self.value = float(value)
