import igraph
from time import time

print(igraph.__version__)


def load_authors(filename):
    authors = []
    with open(filename, "r") as ins:
        for line in ins:
            authors.append(line.split('\t')[1].split(';'))
    return authors

n_authors = 0
authors_indices = {}

def get_author_index(author):
    global n_authors, authors_indices
    if author not in authors_indices:
        authors_indices[author] = n_authors
        n_authors += 1
    return authors_indices[author]


authors = load_authors('datos.csv')
print('Loaded authors...')

for document in authors:
    for author in document:
        get_author_index(author)
print('N_authors: %s' % n_authors)

graph = igraph.Graph()
graph.add_vertices(n_authors)
print('Added vertices...')

indices_authors = []
for document in authors:
    indices_authors.append([get_author_index(a) for a in document])

print('translated to indices...')
n_edges = 0
all_pairs = []
for document in indices_authors:
    pairs = []
    while len(document):
        author = document.pop()
        for author2 in document:
            pairs.append((author, author2))
            n_edges += 1
    #graph.add_edges(pairs)
    print('added edge: %s, pairs added: %s' % (n_edges, len(pairs)))
    all_pairs += pairs
graph.add_edges(all_pairs)
print('added %s edges!' % graph.ecount())
print('Graph before simplify, %s nodes, %s edges' % (graph.vcount(), graph.ecount()))
graph = graph.simplify()
print('Graph after simplify, %s nodes, %s edges' % (graph.vcount(), graph.ecount()))
graphs = graph.decompose()
print('Decomposed in: %s graphs' % len(graphs))
g_index = 0
for g in graphs:
    g_index += 1
    print("Subgraph %s, Nodes: %s, Edges: %s, More than one node: %s" % (g_index, g.vcount(), g.ecount(), g.ecount() > 0))
g_index = 0
for g in graphs:
    print("Subgraph %s, Nodes: %s, Edges: %s, More than one node: %s" % (g_index, g.vcount(), g.ecount(), g.ecount() > 0))
    g_index += 1
    if g.ecount() == 0:
        print("No edges, skip!")
        continue
    start = time()
    g.community_multilevel()
    print("Louvain: %s seconds" % (time() - start)) # Louvain ? https://stackoverflow.com/questions/26070021/how-do-i-run-the-louvain-community-detection-algorithm-in-igraph
    start = time()
    g.community_fastgreedy()
    print("community_fastgreedy: %s seconds" % (time() - start))
    start = time()
    g.community_walktrap()
    print("community_walktrap: %s seconds" % (time() - start))
    start = time()
    g.community_leading_eigenvector()
    print("community_leading_eigenvector: %s seconds" % (time() - start))
    start = time()
    g.community_spinglass()
    print("community_spinglass: %s seconds" % (time() - start))
    # start = time()
    # g.community_optimal_modularity() 
    # print("community_optimal_modularity: %s seconds" % (time() - start))