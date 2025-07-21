import pipeline.reader as reader
from pipeline.node_sensor_manager import NodeSensorManager


def print_data(node_id, tar_path):
    data = reader.data_for_node(node_id, tar_path)
    for row in data:
        print(row)

def print_first_and_next_readings(node_id, tar_path, allowed_offset_seconds=0):
    manager = NodeSensorManager(node_id, tar_path)
    print(f"First readings for node {node_id}:")
    print(manager.current_readings)
    print("\nNext readings:")
    for _ in range(5):  # Print next 5 readings as a demo
        next_reading = manager.next_readings(allowed_offset_seconds=allowed_offset_seconds)
        print(next_reading)

if __name__ == "__main__":
    print_first_and_next_readings(2, 'TarFiles/', allowed_offset_seconds=40)