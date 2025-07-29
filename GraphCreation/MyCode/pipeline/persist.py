import os
import json
import pickle

class GraphStorage:
    """
    A class to store and retrieve graph objects using pickle serialization.
    """
    
    def __init__(self, storage_dir='graph_storage', input_queue=None):
        """
        Initialize the GraphStorage with a directory for storing pickle files.
        
        Args:
            storage_dir (str): Directory where pickle files will be stored
        """
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self.input_queue = input_queue
    def save_graph(self, node_id, graph, filename=None):
        """
        Save a single graph object using pickle.
        
        Args:
            node_id: Identifier for the graph/node
            graph: The graph object to save
            filename (str, optional): Custom filename. If None, uses f'node_{node_id}.pkl'
        """
        if filename is None:
            filename = f'node_{node_id}.pkl'
        
        filepath = os.path.join(self.storage_dir, filename)
        with open(filepath, 'wb') as f:
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
        print(f'Saved {len(graphs)} graphs to {self.storage_dir}')
    
    def load_graph(self, node_id, filename=None):
        """
        Load a single graph object from pickle file.
        
        Args:
            node_id: Identifier for the graph/node
            filename (str, optional): Custom filename. If None, uses f'node_{node_id}.pkl'
            
        Returns:
            The loaded graph object
        """
        if filename is None:
            filename = f'node_{node_id}.pkl'
        
        filepath = os.path.join(self.storage_dir, filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Graph file not found: {filepath}")
        
        with open(filepath, 'rb') as f:
            graph = pickle.load(f)
        print(f'Loaded graph for node {node_id} from {filepath}')
        return graph
    
    def load_graphs(self, node_ids=None):
        """
        Load multiple graph objects from pickle files.
        
        Args:
            node_ids (list, optional): List of node IDs to load. If None, loads all .pkl files in storage_dir
            
        Returns:
            dict: Dictionary of {node_id: graph_object} pairs
        """
        graphs = {}
        
        if node_ids is None:
            # Load all .pkl files in the storage directory
            for filename in os.listdir(self.storage_dir):
                if filename.endswith('.pkl'):
                    node_id = filename.replace('node_', '').replace('.pkl', '')
                    graphs[node_id] = self.load_graph(node_id, filename)
        else:
            # Load specific node IDs
            for node_id in node_ids:
                graphs[node_id] = self.load_graph(node_id)
        
        return graphs
    
    def list_stored_graphs(self):
        """
        List all stored graph files.
        
        Returns:
            list: List of stored graph filenames
        """
        stored_files = []
        for filename in os.listdir(self.storage_dir):
            if filename.endswith('.pkl'):
                stored_files.append(filename)
        return stored_files
    
    def run(self):
        """
        Continuously process graphs from input_queue until a None value is received.
        
        Args:
            input_queue: Queue-like object with get() method that returns (node_id, graph) tuples
                        or None to signal end of processing
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
                else:
                    print(f"Warning: Unexpected item format: {type(item)}")
                    continue
                    
            except Exception as e:
                print(f"Error processing graph: {e}")
                continue
        
        print(f"GraphStorage run completed. Total graphs processed: {processed_count}")


if __name__ == '__main__':
    from graph_emitter import emit_graphs
    from graph_builder import build_graph_for_node
    from read_and_emit import read_state_file, STATE_FILE
    node_series = read_state_file(STATE_FILE)
    graphs = {node_id: build_graph_for_node(series) for node_id, series in node_series.items()}
    emitted_graphs = emit_graphs(graphs)
    persist_graphs(emitted_graphs) 