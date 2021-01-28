# S-Graffito
## Streaming Graph Processing At Scale

S-Graffito is a Streaming Graph Management System that addresses the processing of OLTP and OLAP queries on high streaming rate, very large graphs. These graphs are increasingly being deployed to capture relationships between entities (e.g., customers and catalog items in an online retail environment) both for transactional processing and for analytics (e.g., recommendation systems)

<img src="images/s-graffito-architecture.png?raw=true"/>

### Streaming Graph Querying

Existing work on streaming graph systems, by and large,focuses on either:
 1. Maintenance of graph snapshots under a stream of updates for iterative graph analytic workloads, or
 2. Specialized systems for persistent query workloads that are tailored for the task in hand
 
In a large number of applications, the unbounded nature of streaming graphs and the need for real-time answers on recent data make it impractical to employ snapshot-based techniques.
Specialized systems, on the other hand, provide satisfactory performance for the task in hand but they lack the flexibility to support a wide range of real-world scenarios.

The primary focus of this component is the efficient processing of persistent graph queries over large streaming graphs with very high edge arrival rates.
We investigate query execution techniques and robust system architectures for an efficient and scalable treaming Graph Management System (SGMS).
In particular, we tackle following problems for efficient persistent query evaluation over streaming graphs:
 1. Design and development of non-blocking algorithms for persistent evaluation of path navigation queries over streaming graphs.
 2. Query processing techniques for persistent graph queries with both structural and attribute-based predicates.
 3. Scale-out system architectures and distributed query evaluation techniques to scale to large streaming graphs arising in real-world applications.

### Streaming Graph Analytics

 Graph analytics is concerned with estimating properties of the graph or finding patterns within a graph (e.g. finding cliques or densely connected clusters, subgraph matching, and finding frequent patterns/motifs). Running analytics tasks over streaming graphs is particularly challenging because of the unboundedness of the graph (i.e. sequential access to the unbounded structural events in the graph) as well as the potentially bursty and high velocity arrivals. 

The growing need to process streaming graphs, with their ever-changing nature, has brought about a resurgence of interest in prediction-based analytics over streaming graph (e.g. link prediction, node prediction, event time prediction, and pattern prediction). Prediction-based analytics demand rethinking about processing methods. Classical graph analytics systems operate on static graphs and/or rely on computing explicit structural patterns of the graph (e.g. degree distribution, shortest paths, Pagerank computation, random-walk ranks, connected components, and subgraph counting). Prediction-based analytics, however, requires recognizing hidden/implicit patterns and correlations in the graph. These patterns/correlations rely on both the structure and the properties (i.e. feature vectors of edges/vertices) of the graph. Therefore, prediction-based analytics demands efficiently combining the structure and properties. This is where machine learning can assist graph analytics via graph embedding, a commonly practiced approach to learn a model for mapping the semi-structured graph data to low-dimensional vector space such that the learned embeddings preserve the graph characteristics. Such graph embeddings can adapt to emerging vertices, edges, and the local/global topology of the graph; and they can be adjusted to combine the properties of neighborhoods with any level of depth and width into low dimensional embeddings. The low dimensional space would enhance the performance of the analytics in terms of both memory and latency.

The primary focus of this component is creating an analytics engine that ingests streaming records,  batches them using sliding window semantics,  and performs (several) machine learning-aided analytics tasks on each batch before retiring the corresponding window and ingesting the next batch.  To this end, we design efficient algorithms for a generic analytics engine that is based on time-based windows (as the computation methodology) and low dimensional vertex embeddings (as the analytics primitives). In particular, we tackle the following problems for efficient analytics over streaming graphs.

1. Exploratory analysis of real-world streaming graphs
2. Representation learning over streaming graphs
3. Prediction-based analytics over streaming graphs

## Talks

##### Streaming Graph Processing and Analytics ([Slides](files/streaming_graph_debs_keynote.pdf?raw=true), [Video](https://acm-org.zoom.us/rec/play/vscpde2r-Gk3TNWVtASDBPN7W461LqysgSgf__ZfyxywBSJQM1GhYrITa-O09rqfGKnBoXqR08hHShef)) 
Keynote at [14th International Conference on Distributed and Event-Based Systems, 2020](https://2020.debs.org/)

## Publications

##### Streaming Graph Querying
SIGMOD'20: [Regular Path Query Evaluation on Streaming Graphs](https://arxiv.org/abs/2004.02012)
SIGMOD'19: [Experimental Analysis of Streaming Algorithms for Graph Partitining](https://dl.acm.org/authorize?N697045)

##### Streaming Graph Analytics
arXiv: [sGrapp: Butterfly Approximation in Streaming Graphs]()

## Artifacts

##### Streaming Graph Analytics
[sGrapp: Butterfly Approximation in Streaming Graphs](https://github.com/aidasheshbolouki/sGrapp)


## People

[M. Tamer Özsu](https://cs.uwaterloo.ca/~tozsu/)

[Anil Pacaci](https://cs.uwaterloo.ca/~apacaci/) (PhD Student)

[Aida Sheshbolouki](https://scholar.google.com/citations?user=5zwbvpoAAAAJ&hl=en) (PhD Student)
