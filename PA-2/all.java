import java.io.*; 
import java.util.List;
import java.util.ArrayList;
import java.util.*; 

class test { 

    private ArrayList< ArrayList<String> > cluster;
    private ArrayList<String> indices;
    private ArrayList< ArrayList<String> > ali;

    public test() { 
        cluster = new ArrayList<ArrayList<String>>();
        indices = new ArrayList<String>();
        ali = new ArrayList<ArrayList<String>>();
    } 

    public void addEdge( String src, String dest) {		
		if (indices.indexOf(src) == -1) {
			indices.add(src);
			cluster.add(new ArrayList<String>());
			cluster.get( cluster.size()-1 ).add(dest);
		}
		else {
			cluster.get(indices.indexOf(src)).add(dest);
		}
		
		if (indices.indexOf(dest) == -1) {
			indices.add(dest);
			cluster.add(new ArrayList<String>());
			cluster.get( cluster.size()-1 ).add(src);
		}
		else {
			cluster.get(indices.indexOf(dest)).add(src);
		}
    }
    
    public void DFSUtil(int v, ArrayList<String> visited) {
        visited.set(v, "true");
        ali.get(ali.size()-1).add(indices.get(v));
        
        for (String s : cluster.get(v)) {
			int x = indices.indexOf(s);
			if (x != -1) {
				if(visited.get(x) == "false") DFSUtil(x,visited);
			}
        }
    }
    
    public ArrayList< ArrayList<String> > connectedComponents() { 
		int len = indices.size();
        ArrayList<String> visited = new ArrayList<String>();
        for (int i = 0; i < len; i++){
			visited.add("false");
		}

        for(int v = 0; v < len; ++v) { 
			if (ali.isEmpty()) {
				ali.add(new ArrayList<String>());
			}
			else if (ali.get(ali.size()-1).size() > 0) {
				ali.add(new ArrayList<String>());
			}
            if(visited.get(v) == "false") { 
                DFSUtil(v,visited);
            }
        }
        
        if (ali.get(ali.size()-1).size() == 0) {
			ali.remove(ali.size()-1);
		}
		return ali;
    }
}



class Graph { 
  
    private ArrayList< ArrayList<String> > adjList; 
    private ArrayList<String> indices;
    private Set<String> paths;
  
    // Constructor 
    public Graph(int vertices) {   
        adjList = new ArrayList< ArrayList<String> >();
        indices = new ArrayList<String>();
        paths = new HashSet<String>();
    }
    
    public Set<String> getPaths() {
		return paths;
	}
  
    public void addEdge(String u, String v) { 
        if (indices.indexOf(u) == -1) {
			indices.add(u);
			adjList.add(new ArrayList<String>());
			adjList.get(adjList.size() - 1).add(v);
        }
        else{
			adjList.get(indices.indexOf(u)).add(v);
		}
        
        if (indices.indexOf(v) == -1) {
			indices.add(v);
			adjList.add(new ArrayList<String>());
			adjList.get(adjList.size() - 1).add(u);
		}
        else{
			adjList.get(indices.indexOf(v)).add(u);
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
}


  
class Combination { 
  
    static void combinationUtil(ArrayList<String> s, String data[], int start, int end, int index, ArrayList<String> out) { 
        if (index == 2) { 
            for (int j=0; j<2; j++) {
                out.add(data[j]);
			}
            return;
        } 
  
        for (int i=start; i<=end && end-i+1 >= 2-index; i++) { 
            data[index] = s.get(i); 
            combinationUtil(s, data, i+1, end, index+1, out); 
        } 
    } 
  
    static void Combinations(ArrayList<String> s, int n, ArrayList<String> out) { 
        String data[]=new String[2]; 
        combinationUtil(s, data, 0, n-1, 0, out); 
    }
}


class all {
	
    public static void main (String[] args) throws FileNotFoundException { 
		Combination combat = new Combination();
        ArrayList<String> s = new ArrayList<String>();
        s.add("1");
        s.add("5");
        s.add("6");
        //s.add("7");
        int n = s.size(); 
        ArrayList<String> out = new ArrayList<String>();
        combat.Combinations(s, n, out); 

        for (int j = 0; j < out.size()/2; j++) {
			System.out.println(out.get(2*j) + " }{ " + out.get(2*j+1));
		}
		
		String st = "312~34~56";
		String[] str = st.split("~",3);
		for (String tr : str) {
			System.out.println(tr);
		}
		
		System.out.println(str[0] + "~" + str[2]);
		
		String v = "100274923759";
		String k = "20027492375";
		
		if (k.length() > v.length() || (k.length() == v.length() && k.compareTo(v) > 0) ) {
			System.out.println(v + "~" + k);
		}
		
		else {
			System.out.println(k + "~" + v);
		}
		
		
		
        test g = new test();
          
        g.addEdge("0", "1");
        g.addEdge("0", "2");
        g.addEdge("0", "17");  
        g.addEdge("1", "18");
        g.addEdge("1", "5");
        g.addEdge("1", "6");
        g.addEdge("2", "7");  
        g.addEdge("2", "8");
        g.addEdge("2", "9");
        g.addEdge("3", "10");
        g.addEdge("3", "11");  
        g.addEdge("3", "12");
        g.addEdge("4", "13");
        g.addEdge("4", "14");
        g.addEdge("4", "15");  
        g.addEdge("5", "16");
        g.addEdge("5", "16");
        g.addEdge("16", "5");

        System.out.println("Following are connected components"); 
        ArrayList< ArrayList<String> > outt = g.connectedComponents();
        
        System.out.println("AND SO WE BEGIN.....");
        for (int i = 0; i < outt.size(); i++) {
			System.out.println(Arrays.toString(outt.get(i).toArray()));
			for (int j = 0; j < outt.get(i).size(); j++) {
				
				System.out.print(outt.get(i).get(j) + " ");
			}
			System.out.println();
		}
        // Create a sample graph 
        Graph gr = new Graph(5); 
        gr.addEdge("0", "1"); 
        gr.addEdge("0", "2"); 
        gr.addEdge("0", "3"); 
        gr.addEdge("2", "0"); 
        gr.addEdge("2", "1"); 
        gr.addEdge("1", "2"); 
        gr.addEdge("1", "3");
        gr.addEdge("1", "4"); 
        gr.addEdge("4", "3");
  
        // arbitrary source 
        String sr = "2"; 
  
        // arbitrary destination 
        String dr = "3"; 
  
        System.out.println("Following are all different paths from " + sr + " to " + dr); 
        gr.AllPaths(sr, dr); 
        
		Set<String> paths = gr.getPaths();
		for (String p : paths) {
			System.out.println(p);
		}	
		long zero = 0, one = 0;
		for (int i = 0; i < 2; i++) {
			File file = new File(args[i]);
			Scanner scan = new Scanner(file);
			if (i==0) {
				zero = Long.parseLong(scan.next());
			}
		}
		
		System.out.println("Where are the drugs?? " + zero);
		zero++;
		System.out.println("Where are the drugs?? " + zero);
		//System.out.println("Where are the drugs?? " + one);
		
		System.out.println("----------------------------------------------------------------------------------");
		System.out.println("|                          | L-gfn     | L-random     | C-gfn     | C-random     |");
		System.out.println("----------------------------------------------------------------------------------");
		System.out.println("| Google+ Friend Network   | ");
		System.out.println("----------------------------------------------------------------------------------");
		
		
		System.out.println("----------------------------------------------------------------------------------");
		System.out.print("|                          | 1           | 2           | 3           | 4           | 5           ");
		System.out.print("|                          | 6           | 7           | 8           | 9           | 10          ");
		System.out.print("|                          | Total Shortest Pathlength");
		System.out.println("----------------------------------------------------------------------------------");
		System.out.println("| Google+ Friend Network   | ");
		System.out.println("----------------------------------------------------------------------------------");
		
		int[] i = new int[10];

		i[4] = 3;
		i[7] = 6;
		
		for (int j = 0; j < 10; j++) {
			System.out.println(i[j]);
		}
    } 
}
























