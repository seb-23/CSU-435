import java.io.*; 
import java.util.*;
import java.util.ArrayList; 
import java.util.List; 
import java.util.Set;
import java.util.HashSet;

class Graph { 
  
    private ArrayList< ArrayList<String> > adjList; 
    private ArrayList<String> indices;
    private Set<String> paths;
  
    // Constructor 
    public Graph() {   
        adjList = new ArrayList< ArrayList<String> >();
        indices = new ArrayList<String>();
        paths = new HashSet<String>();
    }
    
    public String getShortestPath() {
		int min = 11;
		String path = "";
		
		for (String s : paths) {
			int len = 0;
			for (char c : s.toCharArray()) {
				if (c == '~') {
					len++;
				}
			}
			
			if (len==1) {
				return s;
			}
			
			if (len < min) {
				min = len;
				path = s;
			}
		}
		
		
		return path;
	}
  
    public void addEdge(String u, String v) { 
        if (indices.indexOf(u) == -1) {
			indices.add(u);
			adjList.add(new ArrayList<String>());
			adjList.get(adjList.size() - 1).add(v);
        }
        else{
			if (adjList.get(indices.indexOf(u)).indexOf(v) == -1) {
				adjList.get(indices.indexOf(u)).add(v);
			}
		}
		if (indices.indexOf(v) == -1) {
			indices.add(v);
			adjList.add(new ArrayList<String>());
			adjList.get(indices.indexOf(v)).add(u);
		}
		else{
			if (adjList.get(indices.indexOf(v)).indexOf(u) == -1) {
				adjList.get(indices.indexOf(v)).add(u);
			}
		}
		
		System.out.println("Indices:" + indices.size() + " " + Arrays.toString(indices.toArray()));
		if (adjList.size() == 5) {
			System.out.println("AdjList:" + adjList.size() + " " + Arrays.toString(adjList.get(0).toArray()));
			System.out.println("AdjList:" + adjList.size() + " " + Arrays.toString(adjList.get(1).toArray()));
			System.out.println("AdjList:" + adjList.size() + " " + Arrays.toString(adjList.get(2).toArray()));
			System.out.println("AdjList:" + adjList.size() + " " + Arrays.toString(adjList.get(3).toArray()));
			System.out.println("AdjList:" + adjList.size() + " " + Arrays.toString(adjList.get(4).toArray()));
		}
    } 
  
 
    public void AllPaths(String s, String d) {  
        ArrayList<String> pathList = new ArrayList<String>(); 
        pathList.add(s); 

		boolean[] isVisited = new boolean[ indices.size() ];
        AllPathsUtil(s, d, isVisited, pathList); 
    } 
  
    private void AllPathsUtil(String u, String d, boolean[] isVisited, ArrayList<String> localPathList) { 
  
        if (localPathList.size() > 11) {
			return;
		}
		
        if (u.equals(d)) { 
            //System.out.println(localPathList);
            String str = "";
            for (String element : localPathList) {
				str += element + "~";
			}
			paths.add(str.substring(0, str.length()-1));
            return; 
        } 
        
        isVisited[ indices.indexOf(u) ] = true; 
  
        for (String s : adjList.get( indices.indexOf(u) )) {
			int i = indices.indexOf(s);
			if (!isVisited[i]) { 
				localPathList.add(s); 
				AllPathsUtil(s, d, isVisited, localPathList); 
  
				localPathList.remove( localPathList.indexOf(s) ); 
			}
        } 
 
        isVisited[ indices.indexOf(u) ] = false; 
    }  

  
    // Driver program 
    public static void main(String[] args) { 
        // Create a sample graph 
        Graph g = new Graph(); 
        g.addEdge("2", "0"); 
        g.addEdge("2", "1"); 
        g.addEdge("1", "3");
        g.addEdge("1", "4"); 
        g.addEdge("4", "3");
        g.addEdge("0", "1"); 
        g.addEdge("0", "2"); 
        g.addEdge("0", "3"); 
  
        // arbitrary source 
        String s = "3"; 
  
        // arbitrary destination 
        String d = "2"; 

        System.out.println("Following are all different paths from " + s + " to " + d); 
        g.AllPaths(s, d); 
        String seb = g.getShortestPath();
        
		System.out.println(seb);
        
        String S = "1~3~4~7";
        String[] str = S.split("~",0);
        
        for (String c : str) {
			System.out.println(c);
			
			inner:
			for (String db : str) {
				System.out.println("sec");
			
				for (String dbc : str) {
					System.out.println("third");
					break inner;
				}
			}
		}
        
        
    } 
} 
  
