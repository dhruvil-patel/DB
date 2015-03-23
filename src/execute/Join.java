package execute;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Comparator;
import java.util.PriorityQueue;

import lru.*;
import parser.*;


public class Join {

	DBSystem db;
	String firstTable;
	String secondTable;
	static int tableNumber;
	static int firstColumnNumber;
	static int secondColumnNumber;
	int firstInterCount;
	int secondInterCount;
	int actualMemory;
	int memoryPageSize;
	int fileNumber;
	public PriorityQueue<Record> tuples;
	
	public void setDbObject(DBSystem d) {
		db = d;
		actualMemory = db.pageSize * db.noOfPages;
		memoryPageSize = db.pageSize;
	}
	
	public void createInterFile() {
		
		tuples = new PriorityQueue<Record>(100,new Tuples());
				
		String tableName;
		if(tableNumber == 0){	
			tableName = firstTable;
			
			//TODO  set Comparing function for table 1
			
		}	
		else {
			tableName = secondTable;
			
			//TODO Set Comparing function for table 2
			
		}	
		
		
		try {
			File tableFile = new File (db.filePath+"/"+tableName+".csv");
			BufferedReader br = new BufferedReader(new FileReader(tableFile));
			
			fileNumber = 0;
			int memory = 0;
			
			String line ;
			line = br.readLine();
			while(line!=null) {
				
				if(memory + line.length() < actualMemory ) {
					memory += line.length();
					tuples.add(new Record(line));
				}	
				else {
					try {
						File fw = new File(db.filePath+"/"+tableName+"_inter_"+fileNumber);
						BufferedWriter bf = new BufferedWriter(new FileWriter(fw , true));
						Record r;
						while(!tuples.isEmpty()) {
							r = tuples.poll();
							bf.append(r.record);
							bf.newLine();
						}
						
						tuples.clear();
						bf.close();
						
						memory = line.length();
						fileNumber++;
						tuples.add(new Record(line));
						
					}catch(Exception e) {
						
					}

				
				}
				line = br.readLine();	
			}
			try {
				File fw = new File(db.filePath+"/"+tableName+"_inter_"+fileNumber);
				BufferedWriter bf = new BufferedWriter(new FileWriter(fw , true));
				Record r;
				while(!tuples.isEmpty()) {
					r = tuples.poll();
					bf.append(r.record);
					bf.newLine();
				}
				if(tableNumber == 0)
					firstInterCount = fileNumber+1;
				else 
					secondInterCount = fileNumber+1;
				
				tuples.clear();
				bf.close();
				
			}catch(Exception e) {
				
			}
			
			
		}catch(Exception e) {
			
		}
		
				
	}
}



class Tuples implements Comparator<Record> {

	@Override
	public int compare(Record first, Record second) {
		// TODO Auto-generated method stub
	
		int columnIndex;
		String firstSplit[];
		String secondSplit[];
		
	
		firstSplit = first.record.split(",");
		secondSplit = second.record.split(",");
		
		if(Join.tableNumber == 0){
			columnIndex = Join.firstColumnNumber;
		}
		else {
			columnIndex = Join.secondColumnNumber;
		}
			
			
		
		/** Set Data Type for comparator
		 * 
		 */
		
		String dataType = null;
		
		if (dataType.equalsIgnoreCase("integer")) {

			int a = Integer.parseInt(firstSplit[columnIndex]);
			int b = Integer.parseInt(secondSplit[columnIndex]);
			
			return a - b;
		}	
		if(dataType.equalsIgnoreCase("float")) {
			float a = Float.parseFloat(firstSplit[columnIndex]);
			float b = Float.parseFloat(firstSplit[columnIndex]);
			
			return (int)((int) a - b);
		}
		else {
			return firstSplit[columnIndex].compareTo(secondSplit[columnIndex]);
		}
	}
	
}	


class Record {
	String record;
	
	public Record(String l) {
		this.record = l;
	}
}