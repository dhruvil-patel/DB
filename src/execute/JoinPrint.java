package execute;

import java.util.Vector;

import lru.*;
import parser.*;


public class JoinPrint {

	DBSystem db ;
	Vector<String> joinSelectColumn;
	//Vector<Integer> secondSelectColumn;
	
	
	//TODO  set this list
	public void setSelect(Vector<String> list) {
		joinSelectColumn = list;
		
	}
	
	
	public void setDbObject(DBSystem d) {
		db = d;
	}
	
	public void printJoinResult(String firstTable , Vector<Integer> list1, String secondTable , Vector<Integer> list2) {
		
		
		// TODO Set Vector for select columns
		
		
		int  firstSize = list1.size();
		int secondSize = list2.size();
		
		Vector<String> firstTableRows = null ;
		Vector<String> secondTableRows = null;
		
		for(int i = 0; i < firstSize ; i++) {
			firstTableRows.add(db.getRecord(firstTable, list1.get(i)));
		}
		
		for(int i = 0; i < secondSize ; i++) {
			secondTableRows.add(db.getRecord(secondTable, list2.get(i)));
		}
		
		/**
		 * Let k be count of select columns of firstTable and List of select column index is there.
		 * 
		 * Same is there for secondTable
		 * 
		 */
		
		
		// TODO  set k1 k2
		int k;
		int selectSize=0;
		//int k2=0;
		
		
		String []firstSplit;
		String []secondSplit;
		
		String output = "";
		
		
		for(int i=0; i< firstSize; i++) {
			
			firstSplit = firstTableRows.get(i).split(",");
					
			for(int j=0 ; j< secondSize ; j++) {
				
				secondSplit = secondTableRows.get(j).split(",");
				output = "";
				
				for(k=0; k< selectSize ;k++) {
					
					String table = joinSelectColumn.get(i).substring(0,joinSelectColumn.get(i).indexOf('.'));  
					String index = joinSelectColumn.get(i).substring(joinSelectColumn.get(i).indexOf('.')+1);
					
					if(	table.equalsIgnoreCase(firstTable)) {
						output += firstSplit[Integer.parseInt(index)];
						
						
					}
					else {
						output += secondSplit[Integer.parseInt(index)];
					}
					if(k+1 < selectSize)
						output += ",";
						
				}
				System.out.println(output);
			}
		}
		
		
	}
	
	public void printChain(Vector<String> tableNames ){
		
		
	}
}
