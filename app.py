from flask import Flask, request, jsonify
from flask_cors import CORS
from neo4j import GraphDatabase
from collections import defaultdict
# AuraDB connection details
URI = "neo4j+s://9074658a.databases.neo4j.io"
USERNAME = "neo4j"
PASSWORD = "ot8ZH3TU191rcKPBpIIf5XSDq1tLybHTPJP2joS3fuM"

class GraphApp:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_node(self, node_id, keywords, date):
        """
        Add a new node to the graph database.
        
        :param node_id: Unique identifier for the node
        :param keywords: List of keywords associated with the node
        :param date: Date associated with the node
        :return: Confirmation message
        """
        query = """
        CREATE (n:Person {id: $node_id, keywords: $keywords, date: $date})
        """
        with self.driver.session() as session:
            session.run(query, node_id=node_id, keywords=keywords, date=date)
        return f"Node with id {node_id} created."

    def add_edge(self, from_id, to_id, similarity):
        """
        Create an edge between two nodes with a user-specified similarity score.
        
        :param from_id: ID of the source node
        :param to_id: ID of the target node
        :param similarity: Similarity score provided by the user
        :return: Confirmation message
        """
        # Validate similarity score
        try:
            similarity_score = float(similarity)
            if not (0 <= similarity_score <= 1):
                return f"Error: Similarity score must be between 0 and 1. Received: {similarity}"
        except ValueError:
            return f"Error: Invalid similarity score. Must be a number between 0 and 1. Received: {similarity}"
        
        # Create edge query with similarity score
        query = """
        MATCH (a:Person {id: $from_id}), (b:Person {id: $to_id})
        CREATE (a)-[:SIMILAR_WITH {similarity: $similarity}]->(b)
        """
        
        with self.driver.session() as session:
            # First, check if both nodes exist
            check_query = """
            MATCH (a:Person {id: $from_id}), (b:Person {id: $to_id})
            RETURN a, b
            """
            check_result = session.run(check_query, from_id=from_id, to_id=to_id)
            
            if not check_result.single():
                return f"Error: One or both nodes with IDs {from_id} and {to_id} not found."
            
            # Create the edge
            session.run(query, from_id=from_id, to_id=to_id, similarity=similarity_score)
            
        query = """
        MATCH (a:Person {id: $to_id}), (b:Person {id: $from_id})
        CREATE (b)-[:SIMILAR_WITH {similarity: $similarity}]->(a)
        """
        
        with self.driver.session() as session:
            # First, check if both nodes exist
            check_query = """
            MATCH (a:Person {id: $to_id}), (b:Person {id: $from_id})
            RETURN a, b
            """
            check_result = session.run(check_query, from_id=to_id, to_id=from_id)
            
            if not check_result.single():
                return f"Error: One or both nodes with IDs {from_id} and {to_id} not found."
            
            # Create the edge
            session.run(query, from_id=to_id, to_id=from_id, similarity=similarity_score)
        
        return f"Edge created from {from_id} to {to_id} with similarity score {similarity_score}."

    def clean_database(self):
        """
        Delete all nodes and relationships from the database.
        
        :return: Confirmation message
        """
        query = """
        MATCH (n)
        DETACH DELETE n
        """
        with self.driver.session() as session:
            session.run(query)
        return "All nodes and relationships have been deleted."
    
    def get_node(self, node_id):
        """
        Retrieve a specific node by its ID.
        
        :param node_id: ID of the node to retrieve
        :return: Node details or None if not found
        """
        query = """
        MATCH (n:Person {id: $node_id})
        RETURN n.id AS id, n.keywords AS keywords, n.date AS date
        """
        with self.driver.session() as session:
            result = session.run(query, node_id=node_id)
            record = result.single()
            
            return dict(record) if record else None

    def get_all_nodes(self):
        """
        Retrieve all nodes in the database.
        
        :return: List of all nodes
        """
        query = """
        MATCH (n:Person)
        RETURN n.id AS id, n.keywords AS keywords, n.date AS date
        """
        with self.driver.session() as session:
            results = session.run(query)
            return [dict(record) for record in results]

    def get_node_edges(self, node_id):
        """
        Retrieve all edges (relationships) for a specific node.
        
        :param node_id: ID of the node to find edges for
        :return: List of edges connected to the node
        """
        query = """
        MATCH (n:Person {id: $node_id})-[r:FRIENDS_WITH]->(connected:Person)
        RETURN n.id AS from_id, 
               connected.id AS to_id, 
               r.similarity AS similarity
        """
        with self.driver.session() as session:
            results = session.run(query, node_id=node_id)
            return [dict(record) for record in results]

    def get_adjacency_list(self):
        """
        Retrieve the graph data as a complete adjacency list.
        
        :return: Adjacency list representation of the graph
        """
        query = """
        // Get all nodes with their edges
        MATCH (n:Person)
        OPTIONAL MATCH (n)-[r:SIMILAR_WITH]->(connected:Person)
        RETURN 
            n.id AS node_id, 
            n.keywords AS node_keywords, 
            n.date AS node_date,
            connected.id AS connected_id, 
            connected.keywords AS connected_keywords, 
            connected.date AS connected_date,
            r.similarity AS similarity
        ORDER BY node_id, connected_id
        """
        
        # Adjacency list to store the graph structure
        adjacency_list = {}
        
        with self.driver.session() as session:
            results = session.run(query)
            
            for record in results:
                node_id = record['node_id']
                
                # Initialize node if not exists
                if node_id not in adjacency_list:
                    adjacency_list[node_id] = {
                        'node_info': {
                            'id': node_id,
                            'keywords': record['node_keywords'],
                            'date': record['node_date']
                        },
                        'edges': []
                    }
                
                # Add edge if connection exists
                if record['connected_id'] is not None:
                    edge = {
                        'id': record['connected_id'],
                        'keywords': record['connected_keywords'],
                        'date': record['connected_date'],
                        'similarity': record['similarity']
                    }
                    
                    # Avoid duplicate edges
                    if edge not in adjacency_list[node_id]['edges']:
                        adjacency_list[node_id]['edges'].append(edge)
        
        return adjacency_list
    
    def filter_adjacency_list_by_keyword(self, keyword):
        """
        Retrieve the graph data filtered by a specific keyword.
        
        This method returns:
        - Nodes that have the keyword
        - Connected nodes with non-null similarity scores
        
        :param keyword: Keyword to filter nodes and edges
        :return: Filtered adjacency list representation of the graph
        """
        # Normalize the keyword to lowercase for case-insensitive matching
        keyword = keyword.lower()
        
        # Enhanced query to find:
        # 1. Nodes with the keyword
        # 2. Connected nodes with non-null similarity scores
        query = """
        // Find nodes with the keyword
        MATCH (matched:Person)
        WHERE any(kw IN matched.keywords WHERE toLower(kw) CONTAINS $keyword)

        // Find connected nodes with non-null similarity
        OPTIONAL MATCH (matched)-[r:SIMILAR_WITH]->(connected:Person)
        WHERE r.similarity IS NOT NULL

        RETURN DISTINCT
            matched.id AS node_id, 
            matched.keywords AS node_keywords, 
            matched.date AS node_date,
            connected.id AS connected_id, 
            connected.keywords AS connected_keywords, 
            connected.date AS connected_date,
            r.similarity AS similarity
        ORDER BY node_id, connected_id
        """
        
        # Adjacency list to store the filtered graph structure
        adjacency_list = {}
        
        with self.driver.session() as session:
            results = session.run(query, keyword=keyword)
            
            for record in results:
                node_id = record['node_id']
                
                # Initialize node if not exists
                if node_id not in adjacency_list:
                    adjacency_list[node_id] = {
                        'node_info': {
                            'id': node_id,
                            'keywords': record['node_keywords'],
                            'date': record['node_date']
                        },
                        'edges': []
                    }
                
                # Add connected node if exists and has a non-null similarity
                if record['connected_id'] is not None and record['similarity'] is not None:
                    edge = {
                        'id': record['connected_id'],
                        'keywords': record['connected_keywords'],
                        'date': record['connected_date'],
                        'similarity': record['similarity']
                    }
                    
                    # Avoid duplicate edges
                    if edge not in adjacency_list[node_id]['edges']:
                        adjacency_list[node_id]['edges'].append(edge)
        
        return adjacency_list

# Initialize Flask app and GraphApp instance
app = Flask(__name__)
CORS(app) 
port=5000
graph_app = GraphApp(URI, USERNAME, PASSWORD)


@app.route("/get-all-edges", methods=["GET"])
def get_all_edges():
    """
    Endpoint to retrieve all edges in the database.
    """
    results = graph_app.get_all_edges()
    return jsonify(results)

@app.route("/get-adjacency-list", methods=["GET"])
def get_adjacency_list():
    """
    Endpoint to retrieve the graph data as an adjacency list.
    
    Returns a dictionary where:
    - Keys are source node IDs
    - Values are lists of connected nodes with their details and similarity scores
    """
    adjacency_list = graph_app.get_adjacency_list()
    return jsonify(adjacency_list)

@app.route("/add-node", methods=["POST"])
def add_node():
    """
    Endpoint to add a new node to the graph database.
    
    Expected JSON payload:
    {
        "id": "unique_node_id",
        "keywords": ["keyword1", "keyword2"],
        "date": "DD-MM-YYYY"
    }
    """
    data = request.json
    node_id = data.get("id")
    keywords = data.get("keywords", [])
    date = data.get("date")
    
    if not node_id or not isinstance(keywords, list) or not date:
        return jsonify({
            "error": "Invalid input. 'id', 'keywords' (list), and 'date' (DD-MM-YYYY) must be provided."
        }), 400
    
    result = graph_app.add_node(node_id, keywords, date)
    return jsonify({"message": result})

@app.route("/filter-by-keyword", methods=["POST"])
def filter_by_keyword():
    """
    Endpoint to filter graph adjacency list by a specific keyword.
    
    Query Parameters:
    - keyword: Keyword to filter nodes and edges
    
    Returns:
    - Filtered adjacency list containing only nodes and edges 
      that match the given keyword
    """
    # Get keyword from query parameters
    data = request.json
    keyword = data.get("keyword")
    
    # Validate keyword is provided
    if not keyword:
        return jsonify({
            "error": "Keyword parameter is required.",
            "hint": "Please provide a keyword query parameter"
        }), 400
    
    try:
        # Filter adjacency list using the keyword
        filtered_graph = graph_app.filter_adjacency_list_by_keyword(keyword)
        
        # Check if any nodes were found
        if not filtered_graph:
            return jsonify({
                "message": f"No nodes found matching keyword: {keyword}",
                "filtered_graph": {}
            }), 404
        
        return jsonify(filtered_graph)
    
    except Exception as e:
        # Generic error handling
        return jsonify({
            "error": "An error occurred while filtering the graph",
            "details": str(e)
        }), 500



@app.route("/add-edge", methods=["POST"])
def add_edge():
    """
    Endpoint to create an edge between two nodes with a user-specified similarity score.
    
    Expected JSON payload:
    {
        "from_id": "source_node_id",
        "to_id": "target_node_id",
        "similarity": 0.75
    }
    """
    data = request.json
    from_id = data.get("from_id")
    to_id = data.get("to_id")
    similarity = data.get("similarity")
    
    if not from_id or not to_id or similarity is None:
        return jsonify({
            "error": "Invalid input. 'from_id', 'to_id', and 'similarity' must be provided."
        }), 400
    
    result = graph_app.add_edge(from_id, to_id, similarity)
    return jsonify({"message": result})

@app.route("/clean-database", methods=["POST"])
def clean_database():
    """
    Endpoint to clean the entire database by removing all nodes and relationships.
    """
    result = graph_app.clean_database()
    return jsonify({"message": result})

@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return jsonify({"status": "API is running."})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
