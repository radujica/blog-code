import networkx as nx
import matplotlib.pyplot as plt

from infer4 import infer_graph
from pipeline3 import pipeline


def draw_matplotlib(graph_edges):
  edges = nx.DiGraph()
  datasets = set()
  funcs = set()
  for from_, func, to_ in graph_edges:
    for f in from_:
      if f:
        edges.add_edge(f, func)
        datasets.add(f)
        funcs.add(func)
    if to_:
      edges.add_edge(func, to_)
      datasets.add(to_)
      funcs.add(func)

  plt.figure(figsize=(8, 6))
  pos = nx.spring_layout(edges, seed=42)
  nx.draw_networkx_nodes(edges, pos, datasets, node_color="tab:red", node_size=4500)
  nx.draw_networkx_labels(edges, pos, dict(zip(datasets, datasets)), font_size=8)
  nx.draw_networkx_nodes(edges, pos, funcs, node_color="tab:blue", node_size=4500)
  nx.draw_networkx_labels(edges, pos, dict(zip(funcs, funcs)), font_size=8)
  nx.draw_networkx_edges(edges, pos, node_size=4500)
  # make the nodes fit in the figure
  axis = plt.gca()
  axis.set_xlim([1.1*x for x in axis.get_xlim()])
  axis.set_ylim([1.1*y for y in axis.get_ylim()])
  plt.draw()
  plt.show()


draw_matplotlib(infer_graph(pipeline))
