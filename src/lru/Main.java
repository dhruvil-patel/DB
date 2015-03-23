package lru;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class Main {
	public static void main(String[] args) throws Exception{
		int input[] = {0,1,2,1,2,2,3,41,9,39,28,1,30,38,39,31,-1, 42,28};
		
		List<String> lines = new ArrayList<String>();
		RandomAccessFile f = new RandomAccessFile("/tmp/countries.csv", "r");
		String s = new String();
		while((s=f.readLine()) != null) {
			lines.add(s);
		}
		
		DBSystem dbs = new DBSystem();
		dbs.readConfig("/tmp/config.txt");
		dbs.populateDBInfo();
		//dbs.print();
		//int i=0;
		for(int inp : input) {
			if(inp !=-1) {
				String op = dbs.getRecord("countries",inp);
				if(lines.get(inp).equals(op)){
					
				} else {
					System.out.println("Fail for record number " + inp + " expected = "+ lines.get(inp) + " actual= " + op);
					//System.exit(-1);
				}
			}
			else
				dbs.insertRecord("countries", "record");
		}
		f.close();
		System.exit(0);
	}
	
}//
/*
public class Main {
 
	
} //*/
/*
public static void main(String []args) {
try{
	PageTable TLB = new PageTable(5);
	
	TLB.lru(1, 1);
	TLB.lru(2, 1);
	TLB.lru(3, 2);
	TLB.lru(4, 3);
	TLB.lru(5, 1);
	//TLB.printPageTable();
	TLB.lru(6, 1);
	TLB.lru(6, 1);
	//TLB.printPageTable();
	TLB.lru(7, 1);
	//TLB.printPageTable();
	TLB.lru(7, 1);
	//TLB.printPageTable();
	TLB.lru(6, 1);
	//TLB.printPageTable();
	TLB.lru(8, 1);
	TLB.lru(8, 1);
	TLB.lru(7, 1);
	TLB.lru(8, 1);
	TLB.lru(9, 1);
	TLB.lru(10, 1);
	TLB.lru(11, 1);
	TLB.lru(7, 1);
	TLB.lru(8, 1);
	TLB.lru(1, 1);
	//TLB.printPageTable();
	
	
	
}catch(Exception e){
	;
}
}*/