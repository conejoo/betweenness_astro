import networkx as nx

def load_authors(filename):
    authors = []
    with open(filename, "r") as ins:
        for line in ins:
            authors.append(line.split('\t')[1].split(';'))
    return authors

graph = nx.Graph()
authors = load_authors('datos.csv')
for document in authors:
    while len(document):
        author = document.pop()
        for author2 in document:
            graph.add_edge(author, author2)

graph = graph.to_undirected()
print("nodes: %s" % graph.number_of_nodes())
print("edges: %s" % graph.number_of_edges())

nx.betweenness_centrality(graph)
