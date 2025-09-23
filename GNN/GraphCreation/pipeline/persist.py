import os
import json
import pickle

class GraphStorage:
    """
    A class to store and retrieve graph objects using pickle serialization.
    """
    
    def __init__(self, storage_dir='graph_storage', input_queue=None, filename='all_graphs.pkl'):
        """
        Initialize the GraphStorage with a directory for storing pickle files.
        
        Args:
            storage_dir (str): Directory where pickle files will be stored
        """
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self.input_queue = input_queue
        self.filename = filename
        
    def save_graph(self, node_id, graph):
        """
        Save a single graph object using pickle.
        
        Args:
            node_id: Identifier for the graph/node
            graph: The graph object to save
        """
        
        filepath = os.path.join(self.storage_dir, self.filename)
        with open(filepath, 'ab') as f:
            pickle.dump(graph, f)
        print(f'Saved graph for node {node_id} to {filepath}')
    
    def save_graphs(self, graphs):
        """
        Save multiple graph objects using pickle.
        
        Args:
            graphs (dict): Dictionary of {node_id: graph_object} pairs
        """
        for node_id, graph in graphs.items():
            self.save_graph(node_id, graph)
        print(f'Saved {len(graphs)} graphs to {self.storage_dir}/{self.filename}')
    
    def load_graphs(self):
        """
        Load all graph objects from pickle file.
        
        Args:
            
        Returns:
            Generator yielding all loaded graph objects
        """
        filepath = os.path.join(self.storage_dir, self.filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Graph file not found: {filepath}")
        
        loaded_count = 0
        with open(filepath, 'rb') as f:
            while True:
                try:
                    graph = pickle.load(f)
                    loaded_count += 1
                    yield graph
                except EOFError:
                    break
        
        print(f'Loaded {loaded_count} graphs from {filepath}')    
    
    def run(self):
        """
        Continuously process graphs from input_queue until a None value is received.
        
        Args:
            input_queue: Queue-like object with get() method that returns:
                        - (node_id, graph) tuples
                        - dict of {node_id: graph} pairs
                        - networkx.Graph objects (will use auto-generated node_id)
                        - None to signal end of processing
        """
        print(f"Starting GraphStorage run loop, saving to {self.storage_dir}")
        processed_count = 0
        
        while True:
            try:
                # Get item from queue
                item = self.input_queue.get()
                
                # Check for None signal to stop
                if item is None:
                    print(f"Received None signal, stopping GraphStorage. Processed {processed_count} graphs.")
                    break
                
                # Process the graph
                if isinstance(item, tuple) and len(item) == 2:
                    node_id, graph = item
                    self.save_graph(node_id, graph)
                    processed_count += 1
                elif isinstance(item, dict):
                    # Handle dictionary of graphs
                    self.save_graphs(item)
                    processed_count += len(item)
                elif hasattr(item, 'number_of_nodes') and hasattr(item, 'number_of_edges'):
                    # Handle networkx.Graph objects directly
                    import networkx as nx
                    if isinstance(item, nx.Graph):
                        # Generate a unique node_id based on graph properties
                        node_id = f"graph_{item.number_of_nodes()}_{item.number_of_edges()}_{processed_count}"
                        self.save_graph(node_id, item)
                        processed_count += 1
                    else:
                        print(f"Warning: Object has graph-like interface but is not networkx.Graph: {type(item)}")
                        continue
                else:
                    print(f"Warning: Unexpected item format: {type(item)}")
                    continue
                    
            except Exception as e:
                print(f"Error processing graph: {e}")
                continue
        
        print(f"GraphStorage run completed. Total graphs processed: {processed_count}")


def test_graph_storage():
    """
    Test the GraphStorage class with graphs created by the graph builder.
    """
    import tempfile
    import shutil
    from queue import Queue
    import threading
    import time
    
    print("=== Testing GraphStorage with Single-File Pickle Serialization ===")
    
    # Create a temporary directory for testing
    test_dir = tempfile.mkdtemp(prefix="graph_storage_test_")
    print(f"Using test directory: {test_dir}")
    
    try:
        # Create sample state data (similar to test_graph_builder)
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
        
        # Create graph builder and generate test graphs
        from pipeline.graph_builder import GraphBuilder
        
        # Create test graphs
        graph_builder = GraphBuilder(None, None)
        hierarchical_graph = graph_builder.build_graph_rack_clique(sample_state)
        nn_s_graph = graph_builder.build_graph_nn_s(sample_state)
        
        print(f"Created hierarchical graph with {hierarchical_graph.number_of_nodes()} nodes and {hierarchical_graph.number_of_edges()} edges")
        print(f"Created nn_s graph with {nn_s_graph.number_of_nodes()} nodes and {nn_s_graph.number_of_edges()} edges")
        
        # Test 1: Basic save and load functionality
        print("\n--- Test 1: Basic Save and Load ---")
        storage = GraphStorage(os.path.join(test_dir, "test_storage"))
        
        # Save individual graphs
        storage.save_graph("hierarchical", hierarchical_graph)
        storage.save_graph("nn_s", nn_s_graph)
        
        # Load and verify graphs (using generator)
        loaded_graphs = list(storage.load_graphs())  # node_id doesn't matter in this approach
        assert len(loaded_graphs) >= 2
        
        # Verify first two graphs are identical
        assert loaded_graphs[0].number_of_nodes() == hierarchical_graph.number_of_nodes()
        assert loaded_graphs[0].number_of_edges() == hierarchical_graph.number_of_edges()
        assert loaded_graphs[1].number_of_nodes() == nn_s_graph.number_of_nodes()
        assert loaded_graphs[1].number_of_edges() == nn_s_graph.number_of_edges()
        
        print("✓ Basic save and load test passed")
        
        # Test 2: Batch save and load
        print("\n--- Test 2: Batch Save and Load ---")
        graphs_dict = {
            "hierarchical_batch": hierarchical_graph,
            "nn_s_batch": nn_s_graph
        }
        
        storage2 = GraphStorage(os.path.join(test_dir, "test_storage2"))
        storage2.save_graphs(graphs_dict)
        loaded_graphs_batch = list(storage2.load_graphs())
        
        assert len(loaded_graphs_batch) >= 2
        print("✓ Batch save and load test passed")
        
        # Test 3: Queue-based processing
        print("\n--- Test 3: Queue-based Processing ---")
        input_queue = Queue()
        storage_with_queue = GraphStorage(os.path.join(test_dir, "queue_storage"), input_queue)
        
        # Start the storage processor in a separate thread
        storage_thread = threading.Thread(target=storage_with_queue.run)
        storage_thread.start()
        
        # Add graphs to the queue
        input_queue.put(("queue_hierarchical", hierarchical_graph))
        input_queue.put(("queue_nn_s", nn_s_graph))
        input_queue.put({"queue_batch_1": hierarchical_graph, "queue_batch_2": nn_s_graph})
        
        # Signal end of processing
        input_queue.put(None)
        
        # Wait for the thread to finish
        storage_thread.join()
        
        # Verify graphs were saved
        queue_storage = GraphStorage(os.path.join(test_dir, "queue_storage"))
        queue_graphs = list(queue_storage.load_graphs())
        assert len(queue_graphs) >= 4
        print("✓ Queue-based processing test passed")
        
        # Test 4: Multiple storage instances
        print("\n--- Test 4: Multiple Storage Instances ---")
        storage_a = GraphStorage(os.path.join(test_dir, "storage_a"), filename="graphs_a.pkl")
        storage_b = GraphStorage(os.path.join(test_dir, "storage_b"), filename="graphs_b.pkl")
        
        storage_a.save_graph("test_a", hierarchical_graph)
        storage_b.save_graph("test_b", nn_s_graph)
        
        graphs_a = list(storage_a.load_graphs())
        graphs_b = list(storage_b.load_graphs())
        
        assert len(graphs_a) == 1
        assert len(graphs_b) == 1
        assert graphs_a[0].number_of_nodes() == hierarchical_graph.number_of_nodes()
        assert graphs_b[0].number_of_nodes() == nn_s_graph.number_of_nodes()
        print("✓ Multiple storage instances test passed")
        
        # Test 5: Error handling
        print("\n--- Test 5: Error Handling ---")
        empty_storage = GraphStorage(os.path.join(test_dir, "empty_storage"))
        try:
            graphs = list(empty_storage.load_graphs())
            assert False, "Should have raised FileNotFoundError"
        except FileNotFoundError:
            print("✓ Error handling test passed")
        
        print("\n=== All GraphStorage tests passed! ===")
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        raise
    finally:
        # Clean up test directory
        shutil.rmtree(test_dir)
        print(f"Cleaned up test directory: {test_dir}")

if __name__ == '__main__':
    # Run the test
    test_graph_storage() 