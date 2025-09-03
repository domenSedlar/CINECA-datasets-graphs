import importlib
import networkx as nx
import json
import matplotlib.pyplot as plt
import numpy as np
import math
from enum import Enum

class GraphTypes(Enum):
    RackClique=0
    NodeClique=1
    NodeTree=2

class GraphBuilder:
    def __init__(self, buffer, output_queue, graph_type=GraphTypes.NodeTree):
        self.buffer = buffer
        self.output_queue = output_queue
        self.graph = None
        self.graphs = {}
        self.sensor_types = ["temp", "pow"]
        self.graph_type = graph_type

    def _get_sensor_id(self, node_id, sensor_name):
        return str(sensor_name) + "_n" + str(node_id)
    
    def _update_graph(self, state):
        if self.graph_type == GraphTypes.RackClique or self.graph_type == GraphTypes.NodeClique:
            for node_id, node_data in state.items():
                # print(node_data)
                for sensor, value in node_data.items():
                    if sensor == "rack_id" or sensor.lower()=="timestamp"or sensor == "node":
                        continue
                    sensor_node_id = self._get_sensor_id(node_id, sensor)
                    self.graph.nodes[sensor_node_id]["value"] = value
        elif self.graph_type == GraphTypes.NodeTree:
            for node_id, node_data in state.items():
                val = node_data.pop("value")
                for sensor, value in node_data.items():

                    if sensor == "rack_id" or sensor.lower()=="timestamp"or sensor == "node":
                        continue
                    sensor_node_id = self._get_sensor_id(node_id, sensor)
                    self.graphs[node_id].nodes[sensor_node_id]["value"] = value
                    self.graphs[node_id].graph["value"] = val
        else:
            print("??, unknown graph type in _update_graph()")

    def build_graph_r_n_sg_s(self, state): # r_n_sg_s: rack_node_sensorGroup_sensor
        graph = nx.Graph()
        rack_nodes = []  # Track all rack nodes to connect them later
        
        # Process each node in the state
        for node_id, node_data in state.items():
            rack_id = node_data['rack_id']
            sensor_data = node_data
            
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
                if sensor_name == "rack_id" or sensor_name.lower()=="timestamp"or sensor_name =="node":
                    continue
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
                        sensor_node = self._get_sensor_id(node_id, sensor_name)
                        graph.add_node(sensor_node, type=sensor_type + "_sensor", 
                                    name=sensor_node, value=sensor_value)
                        graph.add_edge(sensor_type + "_n" + str(node_id), sensor_node)

        # Connect all racks together in a clique
        for i in range(len(rack_nodes)):
            for j in range(i + 1, len(rack_nodes)):
                graph.add_edge(rack_nodes[i], rack_nodes[j])

        return graph
    
    def build_graph(self, graph_type=None, stop_event=None):
        """Build graphs from the state data received in the buffer"""
        state = self.buffer.get()

        # Default to NodeGraph if not specified
        if graph_type is None:
            graph_type = self.graph_type

        # Switch-like structure for graph building and visualization
        if graph_type == GraphTypes.RackClique:
            self.graph = self.build_graph_r_n_sg_s(state)
            visualize = lambda g: self.visualize_graph(g, title="Rack Clique Graph", graph_type="hierarchical")
        elif graph_type == GraphTypes.NodeClique:
            self.graph = self.build_graph_nn_s(state)
            visualize = lambda g: self.visualize_graph(g, title="Node Clique Graph", graph_type="nn_s")
        elif graph_type == GraphTypes.NodeTree:
            for node_id, s in state.items():
                self.build_graph_n_st(node_id=node_id, state=s)
            nd = None
            
            for node_id, s in state.items():
                nd = node_id
                break
            # self.graph = self.graphs[nd]
            visualize = lambda g: self.visualize_node_tree_graph(g, title="Node Clique Graph")
        else:
            raise ValueError(f"Unknown graph_type: {graph_type}")

        # visualize(self.graph)
        i = 0

        while True:
            if stop_event and stop_event.is_set():
                print("graphBuilder detected stop_event set, breaking loop.")
                break
            state = self.buffer.get()
            if state is None:
                print("No more state data to process. Exiting.")
                if self.output_queue is not None:
                    self.output_queue.put(None)
                break
            self._update_graph(state)
            # print(state[2]['timestamp'], state[2]['ambient_avg'])

            # Put the graph in the output queue
            if i < 4:
                #visualize(self.graph)
                i += 1
            if self.output_queue is not None:
                if self.graph is not None:
                    self.output_queue.put(self.graph.copy())
                else:
                    for g in self.graphs.values():
                        # print("g value:", g.graph["value"])
                        self.output_queue.put(g.copy())

    def build_graph_nn_s(self, state): # nn_s: node_sensor, all nodes connected to each other
        graph = nx.Graph()
        node_nodes = []
        
        # First pass: create all nodes and their sensors
        for node_id, node_data in state.items():
            node_node = f"N{node_id}"
            node_nodes.append(node_node)
            graph.add_node(node_node, type='node', id=node_id)
            for sensor_name, sensor_value in node_data.items():
                if sensor_name == "rack_id" or sensor_name.lower()=="timestamp"or sensor_name =="node":
                    continue
                sensor_node = self._get_sensor_id(node_id=node_id, sensor_name=sensor_name)
                graph.add_node(sensor_node, type='sensor', name=sensor_node, value=sensor_value)
                graph.add_edge(node_node, sensor_node)
        
        # Second pass: connect all nodes to each other
        for i in range(len(node_nodes)):
            for j in range(i + 1, len(node_nodes)):
                graph.add_edge(node_nodes[i], node_nodes[j])
        
        return graph
    
    def build_graph_n_st(self, state, node_id): # n_st, a graph for each node, with connections n(id: position) -> st(id: unique, value: x, type: y) id will later be removed
        graph = nx.Graph()
        racks = [
            [48, 47, 46, 45, None, 44, 43, 42, 41, 40, 39, 38, 37,36,35,34,33, None],
            [32, 31, 30, 29, None, 28, 27, 26, 25, None, 24, 23, 22, 21, 20, 19, 18, None],
            [i for i in range(17, -1, -1)]]

        pos_y = None
        pos_x = None

        for i in range(len(racks)):
            for j in range(18):
                if racks[i][j] == state['rack_id']:
                    pos_y = (0, i - 21)
                    if i == 0:
                        pos_x = 10
                    elif i == 1:
                        pos_x = 6
                    else:
                        pos_x = 2

        pos = (pos_x, pos_y)

        graph.add_node(node_id, position=pos)

        val = state.pop("value")

        for sensor_name, value in state.items():
            if sensor_name == "rack_id" or sensor_name.lower()=="timestamp"or sensor_name =="node":
                continue
            stype = "other"
            for i in self.sensor_types:
                if i in sensor_name:
                    stype = i
                    break
            
            graph.add_node(
                self._get_sensor_id(node_id=node_id, sensor_name=sensor_name),
                type=stype,
                value=value
            )
            graph.add_edge(node_id, self._get_sensor_id(node_id=node_id, sensor_name=sensor_name))

        graph.graph["value"] = val

        self.graphs[node_id] = graph


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
            import copy
            # Work on a copy of the graph to avoid modifying the original
            graph_to_draw = graph.copy()
            max_sensors = 3
            # For each node of type 'node', keep only the first 8 connected sensor nodes
            # print(graph_to_draw.nodes)
            sensors_to_remove = []
            for node in list(graph_to_draw.nodes()):
                node_data = graph_to_draw.nodes[node]
                if node_data.get('type') == 'node':
                    # Find connected sensor nodes
                    sensor_neighbors = [n for n in graph_to_draw.neighbors(node)
                                       if graph_to_draw.nodes[n].get('type') in ['sensor', 'temp_sensor', 'power_sensor', 'other_sensor']]
                    sensors_to_remove.append(sensor_neighbors.copy())
            # Only keep the first 8
            for sensor_neighbors in sensors_to_remove:
                for extra_sensor in sensor_neighbors[max_sensors:]:
                    graph_to_draw.remove_node(extra_sensor)
            plt.figure(figsize=(22, 12))
            
            # Define node colors based on type
            node_colors = []
            for node in graph_to_draw.nodes():
                node_data = graph_to_draw.nodes[node]
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
            
            # Build custom labels to show node name and value (if present)
            labels = {}
            for node in graph_to_draw.nodes():
                node_data = graph_to_draw.nodes[node]
                value = node_data.get('value')
                if value is not None:
                    labels[node] = f"{node}\n{value}"
                else:
                    labels[node] = str(node)
            
            # Choose layout based on graph type
            if graph_type == "nn_s":
                pos = self._create_nn_s_layout(graph_to_draw)
            else:
                pos = self._create_hierarchical_layout(graph_to_draw)
            
            # Draw the graph with improved styling
            nx.draw(
                graph_to_draw, pos,
                node_color=node_colors,
                node_size=1200,  # smaller nodes
                font_size=7,     # smaller font
                font_weight='bold',
                with_labels=False,  # We'll use draw_networkx_labels for better control
                edge_color='#2E86AB',
                width=0.8,      # thinner edges
                alpha=0.5,      # more transparent edges
                arrowsize=12,
                arrowstyle='->',
                connectionstyle='arc3,rad=0.1'
            )
            # Draw labels with background for readability
            nx.draw_networkx_labels(
                graph_to_draw, pos, labels,
                font_size=7,
                font_color='black',
                font_weight='bold',
                bbox=dict(facecolor='white', edgecolor='none', boxstyle='round,pad=0.2', alpha=0.7)
            )
            plt.title(title, fontsize=14, fontweight='bold', pad=20)
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
    
    def visualize_node_tree_graph(self, graph, title="Node Tree Graph"):
        """Visualize a NodeTree graph (single node and its sensors)"""
        try:
            plt.figure(figsize=(10, 6))
            pos = nx.spring_layout(graph)
            node_colors = []
            for node in graph.nodes():
                node_data = graph.nodes[node]
                node_type = node_data.get('type', 'unknown')
                if node == list(graph.nodes())[0]:  # Assume first node is the main node
                    node_colors.append('lightgreen')
                elif node_type == 'temp':
                    node_colors.append('orange')
                elif node_type == 'pow':
                    node_colors.append('yellow')
                else:
                    node_colors.append('lightblue')
            labels = {node: f"{node}\n{graph.nodes[node].get('value', '')}" for node in graph.nodes()}
            nx.draw(
                graph, pos,
                node_color=node_colors,
                node_size=1000,
                font_size=8,
                font_weight='bold',
                with_labels=False,
                edge_color='#2E86AB',
                width=1.0,
                alpha=0.8
            )
            nx.draw_networkx_labels(
                graph, pos, labels,
                font_size=8,
                font_color='black',
                font_weight='bold',
                bbox=dict(facecolor='white', edgecolor='none', boxstyle='round,pad=0.2', alpha=0.7)
            )
            plt.title(title, fontsize=14, fontweight='bold', pad=20)
            plt.tight_layout()
            plt.show()
        except ImportError:
            print("matplotlib not available for visualization")
            print("Graph structure:")
            print(f"Nodes: {list(graph.nodes())}")
            print(f"Edges: {list(graph.edges())}")
    
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
                
                angle = 2 * 3.14159 * sensor_index / (len(sensor_nodes)*0.01)
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