import importlib
import networkx as nx
import json
import matplotlib.pyplot as plt
import numpy as np

class GraphBuilder:
    def __init__(self, buffer, output_queue):
        self.buffer = buffer
        self.output_queue = output_queue
        
    def build_graph_r_n_sg_s(self, state): # r_n_sg_s: rack_node_sensorGroup_sensor
        graph = nx.Graph()
        rack_nodes = []  # Track all rack nodes to connect them later
        
        # Process each node in the state
        for node_id, node_data in state.items():
            rack_id = node_data.get('rack_id', 'unknown')
            sensor_data = node_data.get('sensor_data', {})
            
            # Add nodes to the graph
            # R1 (Rack) node
            rack_node = f"R{rack_id}"
            graph.add_node(rack_node, type='rack', id=rack_id)
            # Only add rack node once to avoid duplicates
            if rack_node not in rack_nodes:
                rack_nodes.append(rack_node)
            
            # N0 (Node) node
            node_node = f"N{node_id}"
            graph.add_node(node_node, type='node', id=node_id)
            
            # Connect rack to node
            graph.add_edge(rack_node, node_node)
            
            # Process sensors
            sensors = {"temp": [], "power": [], "other": []}
            


            for sensor_name, sensor_value in sensor_data.items():
                for sensor_type in sensors.keys():
                    if sensor_type in sensor_name.lower() or sensor_type == "other":
                        sensors[sensor_type].append((sensor_name, sensor_value))
                        break

            for sensor_type, sensor_list in sensors.items():
                if sensor_list:
                    sensor_node = sensor_type + "_n" + str(node_id)
                    graph.add_node(sensor_node, type=sensor_type + "_group", sensors=sensor_list)
                    graph.add_edge(node_node, sensor_node)
                    
                    for i, (sensor_name, sensor_value) in enumerate(sensor_list, 1):
                        sensor_node = sensor_name + str(i) + "_n" + str(node_id)
                        graph.add_node(sensor_node, type=sensor_type + "_sensor", 
                                    name=sensor_name, value=sensor_value)
                        graph.add_edge(sensor_type + "_n" + str(node_id), sensor_node)

        # Connect all racks together in a clique
        for i in range(len(rack_nodes)):
            for j in range(i + 1, len(rack_nodes)):
                graph.add_edge(rack_nodes[i], rack_nodes[j])

        return graph
    
    def build_graph(self, graph_type=None):
        """Build graphs from the state data received in the buffer"""
        while True:
            state = self.buffer.get()
            if state is None:
                print("No more state data to process. Exiting.")
                self.output_queue.put(None)
                break
                        
            # Put the graph in the output queue
            self.output_queue.put(self.build_graph_nn_s(state))

    def build_graph_nn_s(self, state): # nn_s: node_sensor, all nodes connected to each other
        graph = nx.Graph()
        node_nodes = []
        
        # First pass: create all nodes and their sensors
        for node_id, node_data in state.items():
            node_node = f"N{node_id}"
            node_nodes.append(node_node)
            graph.add_node(node_node, type='node', id=node_id)
            for i, (sensor_name, sensor_value) in enumerate(node_data.get('sensor_data', {}).items(), 1):
                sensor_node = sensor_name + str(i) + "_n" + str(node_id)
                graph.add_node(sensor_node, type='sensor', name=sensor_name, value=sensor_value)
                graph.add_edge(node_node, sensor_node)
        
        # Second pass: connect all nodes to each other
        for i in range(len(node_nodes)):
            for j in range(i + 1, len(node_nodes)):
                graph.add_edge(node_nodes[i], node_nodes[j])
        
        return graph

    def get_graph_info(self, graph):
        """Get detailed information about the graph"""
        info = {
            'nodes': graph.number_of_nodes(),
            'edges': graph.number_of_edges(),
            'node_types': {},
            'connections': {}
        }
        
        for node in graph.nodes():
            node_data = graph.nodes[node]
            node_type = node_data.get('type', 'unknown')
            if node_type not in info['node_types']:
                info['node_types'][node_type] = []
            info['node_types'][node_type].append(node)
        
        for edge in graph.edges():
            source, target = edge
            if source not in info['connections']:
                info['connections'][source] = []
            info['connections'][source].append(target)
        
        return info
    
    def visualize_graph(self, graph, title="Sensor Network Graph", graph_type="hierarchical"):
        """Visualize the graph using matplotlib"""
        try:
            plt.figure(figsize=(22, 12))
            
            # Define node colors based on type
            node_colors = []
            for node in graph.nodes():
                node_data = graph.nodes[node]
                node_type = node_data.get('type', 'unknown')
                if node_type == 'rack':
                    node_colors.append('lightblue')
                elif node_type == 'node':
                    node_colors.append('lightgreen')
                elif node_type == 'temp_group':
                    node_colors.append('orange')
                elif node_type == 'temp_sensor':
                    node_colors.append('red')
                elif node_type == 'power_group':
                    node_colors.append('yellow')
                elif node_type == 'power_sensor':
                    node_colors.append('darkred')
                elif node_type == 'other_group':
                    node_colors.append('purple')
                elif node_type == 'other_sensor':
                    node_colors.append('pink')
                elif node_type == 'sensor':
                    node_colors.append('red')
                elif node_type == 'isolated':
                    node_colors.append('gray')
                else:
                    node_colors.append('white')
            
            # Choose layout based on graph type
            if graph_type == "nn_s":
                pos = self._create_nn_s_layout(graph)
            else:
                pos = self._create_hierarchical_layout(graph)
            
            # Draw the graph with improved styling
            nx.draw(graph, pos, 
                   node_color=node_colors,
                   node_size=2500,
                   font_size=9,
                   font_weight='bold',
                   with_labels=True,
                   edge_color='#2E86AB',
                   width=1.5,
                   alpha=0.8,
                   arrowsize=15,
                   arrowstyle='->',
                   connectionstyle='arc3,rad=0.1')
            
            plt.title(title, fontsize=16, fontweight='bold', pad=20)
            plt.tight_layout()
            plt.show()
            
        except ImportError:
            print("matplotlib not available for visualization")
            print("Graph structure:")
            print(f"Nodes: {list(graph.nodes())}")
            print(f"Edges: {list(graph.edges())}")
    
    def visualize_nn_s_graph(self, graph, title="Node-to-Node Sensor Graph"):
        """Visualize the nn_s graph type specifically"""
        return self.visualize_graph(graph, title, "nn_s")
    
    def _create_nn_s_layout(self, graph):
        """Create layout for nn_s graph type (node-to-node with sensors)"""
        pos = {}
        
        # Find node nodes and sensor nodes
        node_nodes = [node for node in graph.nodes() if graph.nodes[node].get('type') == 'node']
        sensor_nodes = [node for node in graph.nodes() if graph.nodes[node].get('type') == 'sensor']
        
        # Position node nodes in a circle or line
        for i, node in enumerate(node_nodes):
            angle = 2 * 3.14159 * i / len(node_nodes)
            pos[node] = (3 * np.cos(angle), 3 * np.sin(angle))
        
        # Position sensor nodes around their parent nodes
        for sensor in sensor_nodes:
            # Find parent node
            parent_node = None
            for edge in graph.edges():
                if edge[1] == sensor and graph.nodes[edge[0]].get('type') == 'node':
                    parent_node = edge[0]
                    break
                elif edge[0] == sensor and graph.nodes[edge[1]].get('type') == 'node':
                    parent_node = edge[1]
                    break
            
            if parent_node:
                parent_pos = pos[parent_node]
                # Position sensors in a small circle around their parent
                sensor_index = 0
                for i, s in enumerate(sensor_nodes):
                    if s == sensor:
                        sensor_index = i
                        break
                
                angle = 2 * 3.14159 * sensor_index / len(sensor_nodes)
                pos[sensor] = (parent_pos[0] + 1.5 * np.cos(angle), 
                              parent_pos[1] + 1.5 * np.sin(angle))
        
        return pos
    
    def _create_hierarchical_layout(self, graph):
        """Create a hierarchical layout for better visualization"""
        pos = {}
        
        # Find rack nodes (top level)
        rack_nodes = [node for node in graph.nodes() if graph.nodes[node].get('type') == 'rack']
        
        # Find node nodes (second level)
        node_nodes = [node for node in graph.nodes() if graph.nodes[node].get('type') == 'node']
        
        # Find group nodes (third level)
        group_nodes = [node for node in graph.nodes() if 'group' in graph.nodes[node].get('type', '')]
        
        # Find sensor nodes (fourth level)
        sensor_nodes = [node for node in graph.nodes() if 'sensor' in graph.nodes[node].get('type', '')]
        
        # Find isolated nodes
        isolated_nodes = [node for node in graph.nodes() if graph.nodes[node].get('type') == 'isolated']
        
        # Position rack nodes at the top
        for i, node in enumerate(rack_nodes):
            pos[node] = (i * 3 - len(rack_nodes) * 1.5 + 1.5, 4)
        
        # Position node nodes below racks
        for i, node in enumerate(node_nodes):
            # Find which rack this node belongs to
            parent_rack = None
            for edge in graph.edges():
                if edge[1] == node and graph.nodes[edge[0]].get('type') == 'rack':
                    parent_rack = edge[0]
                    break
                elif edge[0] == node and graph.nodes[edge[1]].get('type') == 'rack':
                    parent_rack = edge[1]
                    break
            
            if parent_rack:
                # Position nodes below their parent rack
                rack_x = pos[parent_rack][0]
                # Calculate offset based on node index within this rack
                nodes_in_rack = [n for n in node_nodes if any(
                    (e[0] == n and e[1] == parent_rack) or (e[1] == n and e[0] == parent_rack)
                    for e in graph.edges()
                )]
                node_index = nodes_in_rack.index(node)
                offset = (node_index - len(nodes_in_rack) / 2 + 0.5) * 1.5
                pos[node] = (rack_x + offset, 3)
            else:
                # Fallback positioning
                pos[node] = (i * 2 - len(node_nodes) + 1, 3)
        
        # Position group nodes below nodes
        for i, node in enumerate(group_nodes):
            # Find which node this group belongs to
            parent_node = None
            for edge in graph.edges():
                if edge[1] == node and graph.nodes[edge[0]].get('type') == 'node':
                    parent_node = edge[0]
                    break
                elif edge[0] == node and graph.nodes[edge[1]].get('type') == 'node':
                    parent_node = edge[1]
                    break
            
            if parent_node:
                # Position groups below their parent nodes
                parent_x = pos[parent_node][0]
                group_type = graph.nodes[node].get('type', '')
                if 'temp' in group_type:
                    pos[node] = (parent_x - 0.5, 2)
                elif 'power' in group_type:
                    pos[node] = (parent_x + 0.5, 2)
                else:
                    pos[node] = (parent_x, 2)
        
        # Position sensor nodes below groups
        for i, node in enumerate(sensor_nodes):
            # Find which group this sensor belongs to
            parent_group = None
            for edge in graph.edges():
                if edge[1] == node and 'group' in graph.nodes[edge[0]].get('type', ''):
                    parent_group = edge[0]
                    break
                elif edge[0] == node and 'group' in graph.nodes[edge[1]].get('type', ''):
                    parent_group = edge[1]
                    break
            
            if parent_group:
                parent_x = pos[parent_group][0]
                # Extract sensor index more reliably
                sensor_index = 1  # default
                for char in node:
                    if char.isdigit():
                        sensor_index = int(char)
                        break
                pos[node] = (parent_x + (sensor_index - 1) * 0.3, 1)
        
        # Position isolated nodes separately
        for i, node in enumerate(isolated_nodes):
            pos[node] = (4, 2)
        
        return pos


if __name__ == '__main__':
    from read_and_emit import read_state_file, STATE_FILE
    node_series = read_state_file(STATE_FILE)
    graphs = {}
    for node_id, series in node_series.items():
        graphs[node_id] = build_graph_for_node(series)
    # For demonstration, print the graph info for each node
    for node_id, graph in graphs.items():
        print(f'Node {node_id}: {graph}') 