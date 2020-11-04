package cps534_2.pageRank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<IntWritable, Text, Text, Text> {
	
	private static final double CONVERGENCE_SCALING_FACTOR = 1000;
	
	public static enum Counter {
		DELTAS // count name, initial value is 0
	}
	
	/* reduce(nid m, [p1,p2,…])
	s = 0; M = ∅
	for all p in [p1,p2,…] do
	if isNode(p) then
	M = p 							// Recover graph structure
	else
	s += p 							// Sum incoming PageRank contributions
	M.pageRank = α /|V| + (1- α)s
	emit(nid m, node M) */
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// TODO: your implementation here, use the pseudo code as instruction
		
		double pagerank = 0.0;
		Node M = null;
		for (Text value : values) {
			Node node = new Node(value.toString());
			if (node.isNode()) { //graph structure
				M = node;
			} else {
				pagerank += node.getPagerank();
			}
		}
		
		// Counter increment
		double delta = M.getPagerank() - pagerank;
		int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));

		
		M.setPagerank(pagerank);
		context.write(new Text(M.getNodeId()), new Text(M.toString()));
		
		
		// e.g., CONVERGENCE_SCALING_FACTOR is 1000
		context.getCounter(Counter.DELTAS).increment(scaledDelta);
	}
}