package cps534_2.pageRank;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		
		/* map(nid n, node N) 							N stores node’s current PageRank and adjacency list
		p = N.pageRank / |N.adjacencyList| 				P(n)/C(n)
		emit(nid n, N) 									Pass along graph structure
		for all nid m in N1adjacencyList do
		emit(nid m, p) 									Pass PageRank mass to neighbors */
		
		// TODO: your implementation here, use the pseudo code as instruction
		
		Node node = new Node(value.toString());
		double p = node.getPagerank() / node.adjacencyListSize();
		context.write(new Text(node.getNodeId()), new Text(node.toString()));
		ArrayList<String> adjList = node.getAdjacencyList();
		for (String adjN : adjList) {
			Node temp_node = new Node(String.valueOf(p));
			context.write(new Text(adjN), new Text(temp_node.toString()));
		}
	}
}