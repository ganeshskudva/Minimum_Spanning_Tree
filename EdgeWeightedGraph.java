package ganzz.apache.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;

public class EdgeWeightedGraph {
    private int V;
    private int E;
    private BidiMap map;
    private Bag<Edge>[] adj;
    
   /**
     * Create an empty edge-weighted graph with V vertices.
     */
    public EdgeWeightedGraph(int V) {
        if (V < 0) throw new RuntimeException("Number of vertices must be nonnegative");
        this.V = V;
        this.E = 0;
        map = new DualHashBidiMap();
        adj = (Bag<Edge>[]) new Bag[V];
        for (int v = 0; v < V; v++) {
            adj[v] = new Bag<Edge>();
        }
    }

   /**
     * Create a random edge-weighted graph with V vertices and E edges.
     * The expected running time is proportional to V + E.
     */
    public EdgeWeightedGraph(int V, int E) {
        this(V);
        if (E < 0) throw new RuntimeException("Number of edges must be nonnegative");
        for (int i = 0; i < E; i++) {
            int v = (int) (Math.random() * V);
            int w = (int) (Math.random() * V);
            double weight = Math.round(100 * Math.random()) / 100.0;
            Edge e = new Edge(v, w, weight);
            addEdge(e);
        }
    }

   /**
     * Create a weighted graph from input stream.
     */
/*    public EdgeWeightedGraph(In in) {
      this(in.readInt());
        int count = 0;
       while(in.hasNextLine())
        {
        	in.nextLine();
        	System.out.println(count++);
        } 
        int E = in.readInt();
      //  int E = count;
        
        for (int i = 0; i < E; i++) {
        	int v = in.readInt();
        	
            int w = in.readInt();
            double weight = in.readDouble();
            Edge e = new Edge(v, w, weight);
            addEdge(e);
        }
    } */
    
    public EdgeWeightedGraph(String s) throws FileNotFoundException{
    	
    	File file = new File(s);
    	Scanner scanner_cp = null, scanner = null,scanner_v=null;
    	int count =0,Edge_cnt=0,u,v;
    	double weigth=0;
    	Set<Integer> set = new TreeSet<Integer>();
    	String[] Tokens;
    	
    	if(file.exists()){
    		scanner = new Scanner(file);
    		scanner_cp = new Scanner(file);
    		scanner_v = new Scanner(file);
    	}
    	
    		
    	
    	/*Count Number of Nodes in the given graph*/
    	while(scanner_v.hasNext())
    	{
    		Tokens = scanner_v.nextLine().split(" ");
    		set.add(Integer.parseInt(Tokens[0]));
    		set.add(Integer.parseInt(Tokens[1]));
    	}
    	
    	/*Initialize the Bag Data Structure*/
    	 if (set.size() < 0) throw new RuntimeException("Number of vertices must be nonnegative");
         this.V = set.size();
         this.E = 0;
         adj = (Bag<Edge>[]) new Bag[V];
         for (int v1 = 0; v1 < V; v1++) {
             adj[v1] = new Bag<Edge>();
         }
    	
    	Tokens = null;
    	
    	/*Count Number of Edges in the given graph*/
    	while(scanner_cp.hasNext())
    	{
    		scanner_cp.nextLine();
    		count++;
    	}
    	
    	Edge_cnt = count;
    //	scanner.reset();
    	
    	for(int i=0; i<Edge_cnt; i++){
    		
    		Tokens = scanner.nextLine().split(" ");
    		u = Integer.parseInt(Tokens[0]);
    		v = Integer.parseInt(Tokens[1]);
    		weigth = Double.parseDouble(Tokens[2]);
    		
    		Edge e = new Edge(u,v,weigth);
    		addEdge(e);
    		
    	}
    	
    }

   /**
     * Copy constructor.
     */
    public EdgeWeightedGraph(EdgeWeightedGraph G) {
        this(G.V());
        this.E = G.E();
        for (int v = 0; v < G.V(); v++) {
            // reverse so that adjacency list is in same order as original
            Stack<Edge> reverse = new Stack<Edge>();
            for (Edge e : G.adj[v]) {
                reverse.push(e);
            }
            for (Edge e : reverse) {
                adj[v].add(e);
            }
        }
    }

   /**
     * Return the number of vertices in this graph.
     */
    public int V() {
        return V;
    }

   /**
     * Return the number of edges in this graph.
     */
    public int E() {
        return E;
    }


   /**
     * Add the undirected edge e to this graph.
     */
    public void addEdge(Edge e) {
        int v = e.either();
        int w = e.other(v);
        int idx = MapGetValue(v);
        adj[idx].add(e);
        idx = MapGetValue(w);
        adj[idx].add(e);
        E++;
    }


   /**
     * Return the edges incident to vertex v as an Iterable.
     * To iterate over the edges incident to vertex v, use foreach notation:
     * <tt>for (Edge e : graph.adj(v))</tt>.
     */
    public Iterable<Edge> adj(int v) {
        return adj[v];
    }

   /**
     * Return all edges in this graph as an Iterable.
     * To iterate over the edges in the graph, use foreach notation:
     * <tt>for (Edge e : G.edges())</tt>.
     */
    public Iterable<Edge> edges() {
        Bag<Edge> list = new Bag<Edge>();
        for (int v = 0; v < V; v++) {
            int selfLoops = 0;
            for (Edge e : adj(v)) {
            	int Mv = MapGetKey(v);
                if (e.other(Mv) > Mv) {
                    list.add(e);
                }
                // only add one copy of each self loop
                else if (e.other(Mv) == Mv) {
                    if (selfLoops % 2 == 0) list.add(e);
                    selfLoops++;
                }
            }
        }
        return list;
    }



   /**
     * Return a string representation of this graph.
     */
    public String toString() {
        String NEWLINE = System.getProperty("line.separator");
        StringBuilder s = new StringBuilder();
        s.append(V + " " + E + NEWLINE);
        for (int v = 0; v < V; v++) {
            s.append(v + ": ");
            for (Edge e : adj[v]) {
                s.append(e + "  ");
            }
            s.append(NEWLINE);
        }
        return s.toString();
    }
    
    public void MapAdd(Object key, int value) {
    	map.put(key, value);
    }
    
    public int MapGetValue(Object key) {
    	return (Integer) map.get(key);
    }
    
    public int MapGetKey(Object value) {
    	return (Integer) map.getKey(value);
    }

   /**
     * Test client.
     */
/*    public static void main(String[] args) {
        In in = new In(args[0]);
        EdgeWeightedGraph G = new EdgeWeightedGraph(in);
        StdOut.println(G);
    } */

}
