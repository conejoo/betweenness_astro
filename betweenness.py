import networkx as nx

def create_graph(filename):
    G = nx.Graph()
    with open(filename, "r") as ins:
        for line in ins:
            n1, n2 = line.strip().split(';')
            G.add_edge(n1,n2)
    return G.to_undirected()


graph = create_graph("episte_grafo.csv")
print "nodes: %s" % graph.number_of_nodes()
print "edges: %s" % graph.number_of_edges()
nx.betweenness_centrality(graph, k=1000)