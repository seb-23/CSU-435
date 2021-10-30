import java.io.*; 
import java.util.List;
import java.util.ArrayList;
import java.util.*; 

class test { 

    private ArrayList< ArrayList<String> > cluster;
    private ArrayList<String> indices;
    private ArrayList< ArrayList<String> > all;
    //private ArrayList< ArrayList<String> > adjList;

    public test() { 
        cluster = new ArrayList<ArrayList<String>>();
        indices = new ArrayList<String>();
        all = new ArrayList<ArrayList<String>>();
        //adjList = new ArrayList<ArrayList<String>>();
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
        all.get(all.size()-1).add(indices.get(v));
        
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
        //System.out.println("Visited:"+Arrays.toString(visited.toArray()));
        //System.out.println("Indices:"+Arrays.toString(indices.toArray()));
        //System.out.println(Arrays.toString(cluster.get(0).toArray()));

        for(int v = 0; v < len; ++v) { 
			if (all.isEmpty()) {
				all.add(new ArrayList<String>());
			}
			else if (all.get(all.size()-1).size() > 0) {
				all.add(new ArrayList<String>());
			}
            if(visited.get(v) == "false") { 
                DFSUtil(v,visited);
            }
        }
        
        if (all.get(all.size()-1).size() == 0) {
			all.remove(all.size()-1);
		}
		return all;
    }
}
  
class Combination { 
  
    static void combinationUtil(ArrayList<String> s, String data[], int start, int end, int index, ArrayList<String> out) { 
        if (index == 2) { 
            for (int j=0; j<2; j++) {
                System.out.print(data[j]+" ");
                out.add(data[j]);
			}
            System.out.println(""); 
            return; 
        } 
  
        for (int i=start; i<=end && end-i+1 >= 2-index; i++) { 
            data[index] = s.get(i); 
            combinationUtil(s, data, i+1, end, index+1, out); 
        } 
    } 
  
    static void printCombination(ArrayList<String> s, int n, ArrayList<String> out) { 
        String data[]=new String[2]; 
        combinationUtil(s, data, 0, n-1, 0, out); 
    }
  
    public static void main (String[] args) { 
        ArrayList<String> s = new ArrayList<String>();
        s.add("1");
        s.add("2");
        s.add("3");
        s.add("4");
        s.add("5");
        s.add("6");
        int n = s.size(); 
        ArrayList<String> out = new ArrayList<String>();
        printCombination(s, n, out); 

		System.out.println();
        for (int j = 0; j < out.size()/2; j++) {
			System.out.println(out.get(2*j) + " " + out.get(2*j+1));
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

        System.out.println("Following are connected components"); 
        ArrayList< ArrayList<String> > rat = g.connectedComponents();
        
        System.out.println("AND SO WE BEGIN.....");
        for (int i = 0; i < rat.size(); i++) {
			System.out.println(Arrays.toString(rat.get(i).toArray()));
		}
		
    } 
} 
