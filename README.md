# Krypton
Massive parallelism computation by Spark.

**Abstract**: Text analysis is a very computation-resource-cost activity with high memory requirement, but normal single computer, which with limited CPU and memory, can not handle it fast. With Spark we can do it better, spark is known well for streaming iterative computation and distributed memory building on large scale of clusters. And with the research paper dataset from PubMed, our idea becomes to category documents by calculating betweenness centrality of words from text network with Spark. The intensive comparative evaluation has been made with three typical indexes in network analysis which are degree centrality, closeness centrality, and betweenness centrality. For a documentation, if the topic of documentation is biology, then a lot of compound nouns with the word “biology” should appear frequently. Therefore, the topics of documents can be presumed by extracting the compound nouns from the document and by focusing on nouns that consist them. The measurement  is the appearance ratio of category name at top n words with high centrality.

**Keywords**: *Spark, BFS, Shortest Path, Betweenness centrality, Text Analysis*

## INTRODUCTION

### Compound Nouns Graph

* Logic View
* Physic View

### Betweenness Centrality
**Betweenness centrality** is an indicator of a node's centrality in a network. It is equal to the number of shortest paths from all vertices to all others that pass through that node. A node with high betweenness centrality has a large influence on the transfer of items through the network, under the assumption that item transfer follows the shortest paths.

<math display='block'>        <mtext>g(v) = </mtext>        <mrow>        <munder>        	<mo>&sum;</mo>          <mrow>        		<mtext>s</mtext>             <mo>&ne;</mo>             <mtext>v</mtext>             <mo>&ne;</mo>             <mtext>t</mtext>          </mrow>        </munder>        <mrow>        <munder>        <mfrac>        <mrow>
        <msubsup><mi>&sigma;</mi> <mi>s,t</mi> <mi></mi></msubsup><mtext>(v)</mtext>        </mrow>        <msubsup><mi>&sigma;</mi> <mi>s,t</mi> <mi></mi></msubsup>        </mfrac>        </munder>        </mrow>        </mrow></math>

where <math><msubsup><mi>&sigma;</mi> <mi>s,t</mi> <mi></mi></msubsup></math> is the number of shortest (s, t)-paths,  and <math><msubsup><mi>&sigma;</mi> <mi>s,t</mi> <mi></mi></msubsup><mtext>(v)</mtext></math> is the number of those paths passing through some  node v other than s, t. If s = t, <math><msubsup><mi>&sigma;</mi> <mi>s,t</mi> <mi></mi></msubsup></math> = 1, and if v in {s, t}, <math><msubsup><mi>&sigma;</mi> <mi>s,t</mi> <mi></mi></msubsup><mtext>(v)</mtext></math> = 0


### Shortest Path Search
Always when running in single thread, we use Dijkstra or Bell-Ford algorithm to find shortest pathes, but in paralle situation, the former 2 algorithm is not easy to implement. But we can use BFS in paralle which maps process on each node to find all shortest pathes.

The idea of algorithm we implemented is from [Ulrik Brandes](http://www.inf.uni-konstanz.de/algo/publications/b-vspbc-08.pdf)

## EXPERIMENTS

### Environment and Datasets
### Performance Summary

## CONCLUSION