import java.util.*; 



class test { 

	private ArrayList< ArrayList<String> > cluster;
	private ArrayList<String> indices;
	private ArrayList< ArrayList<String> > ali;

	test() { 
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
			if (cluster.get(indices.indexOf(src)).indexOf(dest) == -1) {
				cluster.get(indices.indexOf(src)).add(dest);
			}
		}
		
		if (indices.indexOf(dest) == -1) {
			indices.add(dest);
			cluster.add(new ArrayList<String>());
			cluster.get( cluster.size()-1 ).add(src);
		}
		else {
			if (cluster.get(indices.indexOf(dest)).indexOf(src) == -1) {
				cluster.get(indices.indexOf(dest)).add(src);
			}
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
		
		System.out.println("Ali:" + ali.size() + " " + Arrays.toString(ali.get(0).toArray()));
		return ali;
	}

    


      
    public static void main(String[] args){ 
        // Create a test given in the above diagram  
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
        ArrayList< ArrayList<String> > out = g.connectedComponents();
        
        System.out.println("AND SO WE BEGIN.....");
        for (int i = 0; i < out.size(); i++) {
			System.out.println(Arrays.toString(out.get(i).toArray()));
		}
    } 
}     

