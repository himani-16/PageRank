package cps534_2.pageRank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Node {
	
	private boolean isNode = true;
	private String nodeId;
	private double pagerank = 0;
	private ArrayList<String> adjacencyList;

	public Node(String nodeString) {
		String[] nodes = nodeString.split(" ");
		
		// this is a node : "n0	1	a1,a2,a3"
		// ignore dangling node
		try {
			if (nodes.length >= 2) {
				this.nodeId = String.valueOf((nodes[0]));
				this.pagerank = Double.parseDouble(nodes[1]);
				
				ArrayList<String> list = new ArrayList<String>();
				for (String adjLst : nodes[2].split(",")){
					list.add(adjLst);
				}
				this.adjacencyList = list;
				this.isNode = true;
			} else {
				this.isNode = false;
				this.pagerank = Double.parseDouble(nodes[0]);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
	public boolean isNode() {
		return isNode;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public double getPagerank() {
		return pagerank;
	}

	public void setPagerank(double pagerank) {
		this.pagerank = pagerank;
	}

	public ArrayList<String> getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(ArrayList<String> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public boolean containsList() {
		return !adjacencyList.isEmpty();
	}

	public int adjacencyListSize() {
		return adjacencyList.size();
	}

	@Override
	public String toString() {
		String result = "";
		for (String temp: adjacencyList) {
			result = result + temp + ",";
		}
		result = result.substring(0, result.length() - 1);
		return nodeId + "	" + pagerank + "	" + result;
	}
	
	
	/*  public static void main(String[] args) throws Exception {
		try {
             Path pt=new Path("input.txt");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
            	// System.out.println(line);
                
                Node n = new Node(line);
        		// System.out.println(String.valueOf(n.getAdjacencyList()));
        		line = br.readLine();
            } 
            
            Node n = new Node("n1 012 n2,n4");
            String test = n.toString();
            System.out.println("test :" + test);
            
    	} catch(Exception e) {
    		System.out.println(String.valueOf(e));
    	} 
	} */
	
}