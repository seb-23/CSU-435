import java.util.*; 

class SebSort { 

    public String[] sebSorted(String[] str) {
		
		for (int i = 0; i < 2; i++) {
			if (str[i].length() > str[i+1].length() || (str[i].length() == str[i+1].length() && str[i].compareTo(str[i+1]) > 0) ) {
				String tmp = str[i];
				str[i] = str[i+1];
				str[i+1] = tmp;

			}
		}
		
		for (int i = 2; i >0; i--) {
			if (str[i-1].length() > str[i].length() || (str[i-1].length() == str[i].length() && str[i-1].compareTo(str[i]) > 0) ) {
				String tmp = str[i];
				str[i] = str[i-1];
				str[i-1] = tmp;

			}
		}
		return str;
	}

      
    public static void main(String[] args){ 
		//String st = "1~2~3";
		//String st = "1~3~2";
		//String st = "2~1~3";
		//String st = "2~3~1";
		//String st = "3~1~2";
		//String st = "3~2~1";
		String[] str = st.split("~",3);
		SebSort s = new SebSort();
		s.sebSorted(str);
		for (String tr : str) {
			System.out.println(tr);
		}
    } 
}     


