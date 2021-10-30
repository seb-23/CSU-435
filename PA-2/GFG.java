import java.io.*; 
import java.util.*; 
  
// Adjacency list representation of graph 
class Graph  
{ 
    int V; 
    Vector<Integer>[] adj; 
  
    @SuppressWarnings("unchecked") 
    Graph(int V) 
    { 
        this.V = V; 
        this.adj = new Vector[V]; 
  
        for (int i = 0; i < V; i++) 
            adj[i] = new Vector<Integer>(); 
    } 
  
    // add edge to graph 
    void addEdge(int u, int v) 
    { 
        adj[u].add(v); 
        adj[v].add(u); 
    } 
  
    // Returns count of edge in undirected graph 
    int countEdges() 
    { 
        int sum = 0; 
  
        // traverse all vertex 
        for (int i = 0; i < V; i++) 
  
            // add all edge that are linked to the 
            // current vertex 
            sum += adj[i].size(); 
  
        // The count of edge is always even because in 
        // undirected graph every edge is connected 
        // twice between two vertices 
        return sum / 2; 
    } 
} 
  
class GFG  
{ 
  
    // Driver Code 
    public static void main(String[] args) throws IOException  
    { 
        int V = 9; 
        Graph g = new Graph(V); 
  
        // making above uhown graph 
        g.addEdge(0, 1); 
        g.addEdge(1,0);
        g.addEdge(0, 7); 
        g.addEdge(1, 2); 
        g.addEdge(1, 7); 
        g.addEdge(2, 3); 
        g.addEdge(2, 8); 
        g.addEdge(2, 5); 
        g.addEdge(3, 4); 
        g.addEdge(3, 5); 
        g.addEdge(4, 5); 
        g.addEdge(5, 6); 
        g.addEdge(6, 7); 
        g.addEdge(6, 8); 
        g.addEdge(7, 8); 
  
        System.out.println(g.countEdges()); 
    } 
} 
  
// This code is contributed by 
// sanjeev2552 
