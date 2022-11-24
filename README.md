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

 Graph analytics is concerned with estimating properties of the graph or finding patterns within a graph (e.g. finding cliques or densely connected clusters, subgraph matching, and finding frequent patterns/motifs). Running analytics tasks over streaming graphs is particularly challenging because of the unboundedness of the graph (i.e. sequential access to the unbounded structural events in the graph) as well as the potentially bursty and high velocity arrivals. The growing need to process streaming graphs, with their ever-changing nature, has brought about a resurgence of interest in prediction-based analytics over streaming graph (e.g. link prediction, node prediction, event time prediction, and pattern prediction).

The primary focus of this component is creating an analytics engine that ingests streaming records,  batches them using sliding window semantics,  and performs (several) machine learning-aided analytics tasks on each batch before retiring the corresponding window and ingesting the next batch.  To this end, we design efficient algorithms for a generic analytics engine that is based on time-based windows (as the computation methodology) and low dimensional vertex embeddings (as the analytics primitives). In particular, we tackle the following problems for efficient analytics over streaming graphs.

1. Exploratory analysis of real-world streaming graphs
2. Representation learning over streaming graphs
3. Prediction-based analytics over streaming graphs

## Talks

##### Streaming Graph Processing and Analytics ([Slides](files/streaming_graph_debs_keynote.pdf?raw=true), [Video](https://acm-org.zoom.us/rec/play/vscpde2r-Gk3TNWVtASDBPN7W461LqysgSgf__ZfyxywBSJQM1GhYrITa-O09rqfGKnBoXqR08hHShef)) 
Keynote at [14th International Conference on Distributed and Event-Based Systems, 2020](https://2020.debs.org/)

## Publications

##### Streaming Graph Analytics
* A. Sheshbolouki and M. T. Özsu, [sGrapp: Butterfly Approximation in Streaming Graphs](https://dl.acm.org/doi/10.1145/3495011), ACM Transactions on Knowledge Discovery From Data, 2022 (pp. 1-43)

* A. Sheshbolouki and M. T. Özsu, [sGrow: Explaining the Scale-Invariant Strength Assortativity of Streaming Butterflies](https://arxiv.org/abs/2111.12217), ACM Transactions on the Web (TWEB), 2022 (Accepted for publications)

##### Streaming Graph Querying
* A. Pacaci, A. Bonifati and M.T. Özsu, [Evaluating Complex Queries on Streaming Graphs](https://arxiv.org/abs/2101.12305), In Proceedings of the 38th IEEE  International Conference on Data Engineering (ICDE), 2022 (Accepted for publication)

* A. Pacaci, A. Bonifati and M.T. Özsu, [Regular Path Query Evaluation on Streaming Graphs](https://arxiv.org/abs/2004.02012), In Proceedings of the ACM SIGMOD International Conference on Management of Data, 2020 (pp. 1415-1430).

* A. Pacaci and M.T. Özsu,[Experimental Analysis of Streaming Algorithms for Graph Partitining](https://dl.acm.org/authorize?N697045), In Proceedings of the  ACM SIGMOD International Conference on Management of Data, 2019 (pp. 1375-1392)

## Artifacts

##### Streaming Graph Analytics
[sGrapp: Butterfly Approximation in Streaming Graphs]()

[Transient Concepts in Streaming Graphs] (https://github.com/dsg-uwaterloo/s-graffito/tree/master/conceptDrift.zip)

##### Streaming Graph Querying 
[Evaluating Complex Queries on Streaming Graphs](https://github.com/dsg-uwaterloo/s-graffito/tree/master/query-processor)

## People

[M. Tamer Özsu](https://cs.uwaterloo.ca/~tozsu/)

[Angela Bonifati](https://perso.liris.cnrs.fr/angela.bonifati/) (Collaborator at Lyon 1 University)

[Anil Pacaci](https://cs.uwaterloo.ca/~apacaci/) (PhD Student)

[Aida Sheshbolouki](https://aidasheshbolouki.com) (PhD Student)

[Kerem Akillioglu](https://keremakillioglu.github.io) (MMath Student)
