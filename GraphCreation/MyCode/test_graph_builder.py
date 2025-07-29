import json
from queue import Queue
from pipeline.graph_builder import GraphBuilder

def test_graph_builder():
    """Test the graph builder with sample data"""
    
    # Sample state data (similar to what's in the state file)
    sample_state = {
        "3": {
            "timestamp": "2020-03-10T15:45:00+00:00", 
            "rack_id": "0", 
            "sensor_data": {
                "ambient_avg": 19.19081340832944, 
                "dimm0_temp_avg": 25.34220387483225,
            }
        }, 
        "11": {
            "timestamp": "2020-03-10T15:45:00+00:00", 
            "rack_id": "0", 
            "sensor_data": {
                "ambient_avg": 22.92695564739419, 
                "dimm0_temp_avg": 28.832163899708004,
            }
        },
        "7": {
            "timestamp": "2020-03-10T15:45:00+00:00", 
            "rack_id": "1", 
            "sensor_data": {
                "ambient_avg": 21.5,
                "dimm0_temp_avg": 26.8,
                "cpu_temp_avg": 42.1,
                "gpu_temp_avg": 65.4,
                "power_consumption": 118.9,
                "fan_speed": 1650,
            }
        },
        "15": {
            "timestamp": "2020-03-10T15:45:00+00:00", 
            "rack_id": "1", 
            "sensor_data": {
                "ambient_avg": 23.8,
                "dimm0_temp_avg": 29.1,
                "cpu_temp_avg": 50.3,
                "gpu_temp_avg": 74.6,
                "power_consumption": 156.7,
                "fan_speed": 2250,
            }
        }
    }
    
    # Create queues for testing
    input_buffer = Queue()
    output_queue = Queue()
    
    # Create graph builder
    graph_builder = GraphBuilder(None, input_buffer, output_queue)
    
    # Test hierarchical graph
    print("=== Testing Hierarchical Graph (r_n_sg_s) ===")
    hierarchical_graph = graph_builder.build_graph_r_n_sg_s(sample_state)
    graph_builder.visualize_graph(hierarchical_graph, "Hierarchical Sensor Graph")
    
    # Test nn_s graph
    print("\n=== Testing Node-to-Node Graph (nn_s) ===")
    nn_s_graph = graph_builder.build_graph_nn_s(sample_state)
    graph_builder.visualize_nn_s_graph(nn_s_graph, "Node-to-Node Sensor Graph")
    
    # Print graph information
    print("\n=== Hierarchical Graph Info ===")
    info = graph_builder.get_graph_info(hierarchical_graph)
    print(f"Nodes: {info['nodes']}, Edges: {info['edges']}")
    print(f"Node types: {info['node_types']}")
    
    print("\n=== NN_S Graph Info ===")
    info = graph_builder.get_graph_info(nn_s_graph)
    print(f"Nodes: {info['nodes']}, Edges: {info['edges']}")
    print(f"Node types: {info['node_types']}")
    
    return hierarchical_graph, nn_s_graph

if __name__ == "__main__":
    test_graph_builder() 