package lru;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Vector;

import parser.NewQueryParser;
import execute.*;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class DBSystem {
	// readConfig
	public String filePath;
	public int pageSize, noOfPages;
	public int splitTemp = 0;
	public FrameBuffer fb;
	public PageTable TLB;

	// List of DBtables
	public List<String> fileName = new ArrayList<String>();

	/**
	 * Reads ConfigFil and set PageSize and number of PAGEs.
	 * 
	 * @param configFilePath
	 *            : Path of ConfigFile
	 */
	public void readConfig(String configFilePath) {
		//String primaryKey;
		File file = new File(configFilePath);
		Scanner readObj;
		try {
			readObj = new Scanner(file);
			boolean tableNameFlag = false, tableColumnDetail = false;
			List<String> fileColumn = new ArrayList<String>();
			List<String> fileColumnType = new ArrayList<String>();
			while (readObj.hasNextLine()) {
				String line = readObj.nextLine();
				Scanner lineBreaker = new Scanner(line);
				String token;
				if(lineBreaker.hasNext()){
					token = lineBreaker.next();
				}else{
					break;
				}
				
				// System.out.println("token is as "+token);
				if (token.equalsIgnoreCase("page_size")) {
					pageSize = lineBreaker.nextInt();
					 //System.out.println("PageSize is : "+pageSize);
				} else if (token.equalsIgnoreCase("num_pages")) {
					noOfPages = lineBreaker.nextInt();
					 //System.out.println("NO of Pages is : "+noOfPages);
				} else if (token.equalsIgnoreCase("path_for_data")) {
					filePath = lineBreaker.next();
					// System.out.println("file Path is : "+filePath);
				} else if (token.equalsIgnoreCase("begin")) {
					tableNameFlag = true;
				} else if (tableNameFlag) {
					tableNameFlag = false;
					tableColumnDetail = true;
					fileName.add(token);
					// System.out.println("File name is : "+token);
				} else if (token.equalsIgnoreCase("end")) {
					tableColumnDetail = false;
					String currentTable = fileName.get(fileName.size() - 1);
					Scanner readFile = new Scanner(new File(filePath + "/"
							+ currentTable + ".csv"));
					int noOfColumn = fileColumn.size(), i = 0;
					BufferedWriter[] fw = new BufferedWriter[noOfColumn];

					for (String column : fileColumn) {
						fw[i] = new BufferedWriter(new FileWriter(filePath+"/"+currentTable
								+ "_" + column));
						i++;
					}
					int k = 0;
				
					while (readFile.hasNextLine()) {
						String record = readFile.nextLine();
						String []split=record.split(",");
						if(split.length==noOfColumn){
							for (int j = 0; j < noOfColumn; j++) {
								String temp=split[j].trim();
								if(temp.startsWith("\""))
									temp=temp.substring(1,temp.length()-1);
								String insertRecord = temp+ ":"
										+ k + "\n";
								
								fw[j].write(insertRecord);
							}
							k++;	
						}
					}
					k = 0;
					for (; k < noOfColumn; k++) {
						fw[k].close();
					}
					k=0;
					for(String column:fileColumn){
						String type=fileColumnType.get(k);
						
						k++;
						if(type.toLowerCase().trim().startsWith("int")){
							BufferedReader br=new BufferedReader(new FileReader(filePath+"/"+currentTable
									+ "_" + column));
							
							TreeMap<Integer,Vector<Integer>> mapInt=new TreeMap<Integer,Vector<Integer>>();//(myComparator);
							Vector<Integer> myIntVector;
							String rLine=null;
							rLine=br.readLine();
							while(rLine!=null){
								String []split=rLine.split(":");
								Integer key=Integer.parseInt(split[0].trim());
								Integer value=Integer.parseInt(split[1].trim());
								if(mapInt.containsKey(key)){
								myIntVector=mapInt.get(key);
								myIntVector.add(value);
								}
								else{
									myIntVector=new Vector<Integer>();
									myIntVector.add(value);
									mapInt.put(key, myIntVector);
								}
								rLine=br.readLine();
							}
							br.close();
							BufferedWriter bw=new BufferedWriter(new FileWriter(filePath+"/"+currentTable
									+ "_" + column));
							
							Vector<Integer> val;
							for(Integer myKey:mapInt.keySet()){	
								 val=mapInt.get(myKey);
								 String value="";
								while(!val.isEmpty()){
									Integer v=val.firstElement();
									value=value+v+",";
									val.remove(0);
								}
								
								value=value.substring(0,value.length()-1);
								bw.write(myKey+":"+value+"\n");
							}
							bw.close();
							mapInt.clear();
							
						}else if(type.toLowerCase().trim().startsWith("float")){
							BufferedReader br=new BufferedReader(new FileReader(filePath+"/"+currentTable
									+ "_" + column));
							
							TreeMap<Float,Vector<Integer>> mapFloat=new TreeMap<Float,Vector<Integer>>();//(myComparator);
							Vector<Integer> myFloatVector;
							String rLine=null;
							rLine=br.readLine();
							while(rLine!=null){
								String []split=rLine.split(":");
								Float key=Float.parseFloat(split[0].trim());
								Integer value=Integer.parseInt(split[1].trim());
								if(mapFloat.containsKey(key)){
								myFloatVector=mapFloat.get(key);
								myFloatVector.add(value);
								}
								else{
									myFloatVector=new Vector<Integer>();
									myFloatVector.add(value);
									mapFloat.put(key, myFloatVector);
								}
								rLine=br.readLine();
							}
							br.close();
							BufferedWriter bw=new BufferedWriter(new FileWriter(filePath+"/"+currentTable
									+ "_" + column));
							
							Vector<Integer> val;
							for(Float myKey:mapFloat.keySet()){	
								 val=mapFloat.get(myKey);
								 String value="";
								while(!val.isEmpty()){
									Integer v=val.firstElement();
									value=value+v+",";
									val.remove(0);
								}
								
								value=value.substring(0,value.length()-1);
								
								bw.write(myKey+":"+value+"\n");
							}
							bw.close();
							mapFloat.clear();
						}else if(type.toLowerCase().trim().startsWith("varchar")){
							BufferedReader br=new BufferedReader(new FileReader(filePath+"/"+currentTable
									+ "_" + column));
							
							TreeMap<String,Vector<Integer>> mapString=new TreeMap<String,Vector<Integer>>();//(myComparator);
							Vector<Integer> myIntVector;
							String rLine=null;
							rLine=br.readLine();
							while(rLine!=null){
								String []split=rLine.split(":");
								String key=split[0].trim();
								Integer value=Integer.parseInt(split[1].trim());
								if(mapString.containsKey(key)){
								myIntVector=mapString.get(key);
								myIntVector.add(value);
								}
								else{
									myIntVector=new Vector<Integer>();
									myIntVector.add(value);
									mapString.put(key, myIntVector);
								}
								rLine=br.readLine();
							}
							br.close();
							BufferedWriter bw=new BufferedWriter(new FileWriter(filePath+"/"+currentTable
									+ "_" + column));
							
							Vector<Integer> val;
							for(String myKey:mapString.keySet()){	
								 val=mapString.get(myKey);
								 String value="";
								while(!val.isEmpty()){
									Integer v=val.firstElement();
									value=value+v+",";
									val.remove(0);
								}
								
								value=value.substring(0,value.length()-1);
								bw.write(myKey+":"+value+"\n");
							}
							bw.close();
							mapString.clear();
						}
					}
					readFile.close();
					fileColumn.clear();

				} else if (tableColumnDetail) {
					if (token.equalsIgnoreCase("PRIMARY_KEY")) {
						//primaryKey=lineBreaker.next().trim();
					} else {
						String t;
						t=token.substring(0,token.indexOf(","));
						fileColumn.add(t.trim());
						t=token.substring(token.indexOf(",")+1);
						fileColumnType.add(t.trim());
					}
				}
				lineBreaker.close();
			}
			fb = new FrameBuffer(pageSize, noOfPages);
			TLB = new PageTable(noOfPages);
			// System.out.println("inreadconfig"+TLB.pagesOccupied);
		} catch (FileNotFoundException e) {
			;
		} catch (IOException e) {

		}
	}

	// populateDB
	ArrayList<LinkedList<DBinfo>> fileMapping = new ArrayList<LinkedList<DBinfo>>();

	/**
	 * populateDBInfo map records to page number of all file
	 */
	public void populateDBInfo() {
		try {
			int fileCounter = fileName.size(); // number of file in our database
			LinkedList<DBinfo> duplicate;
			DBinfo dbInfoObj;
			
			int i = 0;

			while (fileCounter-- > 0) {
				dbInfoObj = new DBinfo();
				String fileLName = fileName.get(i);
				i++;
				duplicate = new LinkedList<DBinfo>();
				int size = 0, totalSize = 0;
				int recordId = 0; // currentSize=0; //currentSize is used for
									// calculating offset value within page
				File file = new File(filePath + "/" + fileLName + ".csv");
				Scanner readObj;
				readObj = new Scanner(file);
				dbInfoObj.strtRecord = 0;
				dbInfoObj.startingByte = 0;

				while (readObj.hasNextLine()) {
					String line = readObj.nextLine();
					if(line.length()==0)
						continue;
					if (size + line.getBytes().length < pageSize) {
						size = size + 1 + line.getBytes().length;
						// System.out.println("populate DB page is not full");

					} else if (size + line.getBytes().length == pageSize) {
						size = size + line.getBytes().length;
						splitTemp = 1;
					} else {
						// System.out.println("populate DB page is full..........");
						dbInfoObj.endRecord = recordId - 1;
						// dbInfoObj.offset=totalSize-currentSize;
						dbInfoObj.offset = size;
						duplicate.add(dbInfoObj);
						dbInfoObj = new DBinfo();

						if (splitTemp == 1) {

							dbInfoObj.split = 1;
							size = line.getBytes().length + 2; // 1 for \n of
																// current
																// record and
																// another 1 for
																// \n which is
																// of last page
							splitTemp = 0;
						} else {
							size = line.getBytes().length + 1;
						}
						dbInfoObj.strtRecord = recordId;
						dbInfoObj.startingByte = totalSize;
					}
					totalSize = totalSize + 1 + line.getBytes().length;
					recordId++;
				}
				if (dbInfoObj.strtRecord!=-1) {
					dbInfoObj.endRecord = recordId - 1;
					dbInfoObj.offset = size;
					// dbInfoObj.offset=totalSize-currentSize;
					duplicate.add(dbInfoObj);
				}

				fileMapping.add(duplicate);
				readObj.close();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void print() {
		int size = fileMapping.size(), i = 0;
		LinkedList<DBinfo> duplicate;
		DBinfo db;
		// System.out.println("File Mapping"+size);
		while (size-- > 0) {

			duplicate = fileMapping.get(i);
			System.out.println("New file Index");
			int nodeSize = duplicate.size(), j = 0;
			// System.out.println("no of records in file"+nodeSize);
			while (nodeSize-- > 0) {
				db = duplicate.get(j);
				System.out.println(db.strtRecord + " " + db.endRecord + " "
						+ db.offset + "  " + db.startingByte);
				j++;
			} // inner while ended which is used for printing Page info Table
			i++;

		} // outer file after reading all file

	}

	// This function return info of record if valid recordId provided and if
	// recordId = -1 then it holds
	// last Page info

	/**
	 * info of record if valid recordId provided and if recordId = -1 then it
	 * holds last Page info
	 * 
	 * @param tableName
	 * @param recordId
	 * @return RecordInfoj
	 */

	RecordInfo positionInfo(String tableName, int recordId) {
		int counter = 0, noOfTable = fileName.size();
		RecordInfo myRecord = new RecordInfo();
		for (; counter < noOfTable; counter++) { // this loop findout index of
													// tableName
			if (tableName.equals(fileName.get(counter))) {
				myRecord.tableNo = counter;
				break;
			}
		}

		if (myRecord.tableNo == -1) { // if table not exist with tableName as
										// given in parameter
			return myRecord;
		}

		LinkedList<DBinfo> duplicate = fileMapping.get(myRecord.tableNo);
		int nodeSize = duplicate.size(); // nodeSize contains no of node in
											// duplicate
		DBinfo db;
		if (recordId == -1) {
			db = duplicate.getLast();
			myRecord.pageNo = nodeSize - 1;
			myRecord.startLoc = db.startingByte;
			myRecord.offset = db.offset;
			myRecord.split = db.split;
			myRecord.strtRecord = db.strtRecord;
			myRecord.endRecord = db.endRecord;

		} else {
			int i = 0;
			while (nodeSize-- > 0) {
				db = duplicate.get(i);
				if (db.strtRecord <= recordId && db.endRecord >= recordId) {
					myRecord.pageNo = i;
					myRecord.startLoc = db.startingByte;
					myRecord.offset = db.offset;
					myRecord.split = db.split;
					myRecord.strtRecord = db.strtRecord;
					myRecord.endRecord = db.endRecord;
					break;
				}
				i++;
			}
		}

		return myRecord;
	}

	/**
	 * Checks for record in Memory if found return from memory else bring
	 * corresponding page in memory and return the record.
	 * 
	 * @param tableName
	 *            : Table name
	 * @param recordId
	 *            : Record
	 * @return record
	 * 
	 */
	public String getRecord(String tableName, int recordId) {
		String record = "";
		RecordInfo rinfo = positionInfo(tableName, recordId);
		int check = TLB.lru(rinfo.pageNo, rinfo.tableNo);
		
		//System.out.println("Print : "+rinfo);
		//System.out.println(tableName + "   "+ recordId);
		// HIT--- When page is in memory
		if (check >= 0) {
			//System.out.println("HIT");
			record = fb.frameBuffer.elementAt(check).elementAt(
					recordId - rinfo.strtRecord);
			return record;
		} else { // MISS ---
			String str;
			int startRcordNumber = rinfo.strtRecord;
			try {
				File f = new File(filePath +"/"+ tableName + ".csv");
				RandomAccessFile rf = new RandomAccessFile(f, "r");
				
				//System.out.println(rinfo.startLoc);
				
				/**
				 * check for last record
				 */
				
				rf.seek(rinfo.startLoc);

				// MISS---Memory is Empty

				if (check == -1) {
					//System.out.println("MISS " + (TLB.pagesOccupied - 1));
					while (startRcordNumber++ <= rinfo.endRecord) {
						str = rf.readLine();
						if (startRcordNumber - 1 == recordId)
							record = str;
						fb.frameBuffer.elementAt(TLB.pagesOccupied - 1)
								.add(str);
					}
				}

				// MISS---replace frame no = (-check)-2
				else {

					int replaceframe = (-check) - 2;
					//System.out.println("MISS " + replaceframe);
					fb.frameBuffer.elementAt(replaceframe).clear();
					while (startRcordNumber++ <= rinfo.endRecord) {
						str = rf.readLine();
						if (startRcordNumber - 1 == recordId)
							record = str;
						fb.frameBuffer.elementAt(replaceframe).add(str);
					}

					// System.out.print("Insert Page on frame "+check);

				}
				rf.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return record;
		}

	}
	
	
	
	
	
	
	
	private void selectMultiJoin(TSelectSqlStatement q) {
		
		
		
		if(true){
			System.out.println("Multi Join");
			System.out.println(q.joins.getJoin(0).getJoinItems().size());
		}
		
		
		
		Vector <JoinCondition> joinConditions = new Vector<JoinCondition>();
		Vector <String> joinTables = new Vector<String>(q.joins.getJoin(0).getJoinItems().size()+2);
		for(int i=0;i<q.joins.getJoin(0).getJoinItems().size();i++){
			joinConditions.add(new JoinCondition(q.joins.getJoin(0).getJoinItems().getJoinItem(i).getOnCondition()));
			if(i>0){
				if(joinConditions.get(i-1).leftTable.equals(joinConditions.get(i).leftTable) || joinConditions.get(i-1).leftTable.equals(joinConditions.get(i).rightTable)){
					if(i==1)
						joinTables.add(i-1,joinConditions.get(i-1).rightTable);
					joinTables.add(i,joinConditions.get(i-1).leftTable);
					
				}else{
					if(i==1)
						joinTables.add(i-1,joinConditions.get(i-1).leftTable);
					joinTables.add(i,joinConditions.get(i-1).rightTable);
					
				}
			}
			if(i == q.joins.getJoin(0).getJoinItems().size()-1){
				if(joinConditions.get(i).leftTable.equals(joinConditions.get(i-1).leftTable) || joinConditions.get(i).leftTable.equals(joinConditions.get(i-1).rightTable)){
					joinTables.add(i+1,joinConditions.get(i).rightTable);
				}else{
					joinTables.add(i+1,joinConditions.get(i).leftTable);
				}
			}
		}
		if(true)
			System.out.println(joinTables);
		MatricsChainMul m = new MatricsChainMul(joinConditions,joinTables);
	}
	
	
	
	
	
	
	
	
	
	
	

	// /*
	public void insertRecord(String tableName, String r) {
		try {
			int check = noOfPages + 1; // garbage value is inserted into check
			RecordInfo rinfo = positionInfo(tableName, -1);
			String record = r + "\n";
			File f = new File(filePath + "/" + tableName + ".csv"); // *1
			RandomAccessFile rf = new RandomAccessFile(f, "rw");
			LinkedList<DBinfo> duplicate = fileMapping.get(rinfo.tableNo);
			DBinfo db;
			if (rinfo.offset + r.length() <= pageSize) {
				check = TLB.lru(rinfo.pageNo, rinfo.tableNo);
				int replaceframe = 0;

				if (check == -1)
					replaceframe = TLB.pagesOccupied - 1; // page is not in
															// memory but it
															// allocated space
															// to that frame if
															// -1 then call
															// pageOccupied
															// value-1
				else if (check < 0)
					replaceframe = (-check) - 2;
				// otherwise call page data in memory at check value
				if (check < 0) {
					fb.frameBuffer.elementAt(replaceframe).clear();
					rf.seek(rinfo.startLoc);
					int startRcordNumber = rinfo.strtRecord;
					while (startRcordNumber++ <= rinfo.endRecord) {
						fb.frameBuffer.elementAt(replaceframe).add(
								rf.readLine());
					}
					check = replaceframe;
				}
			}

			if (rinfo.offset + r.length() < pageSize) {

				fb.frameBuffer.elementAt(check).add(record);
				rf.seek(rinfo.startLoc + rinfo.offset - rinfo.split);
				rf.writeBytes(record);
				db = duplicate.getLast();
				db.endRecord += 1;
				db.offset += record.length();
				// for test
				// System.out.println(check);
				// TLB.updateRefrence(check);

			} else if (rinfo.offset + r.length() == pageSize) {
				check = TLB.lru(rinfo.pageNo, rinfo.tableNo);
				fb.frameBuffer.elementAt(check).add(r);
				rf.seek(rinfo.startLoc + rinfo.offset - rinfo.split);
				rf.writeBytes(record);
				db = duplicate.getLast();
				db.endRecord += 1;
				db.offset += r.length();
				splitTemp = 1;
				// for test
				// System.out.println(check);
				// TLB.updateRefrence(check);

			} else { // needed new page
				db = new DBinfo(); // new page entry
				db.startingByte = rinfo.offset + rinfo.startLoc + splitTemp;
				db.strtRecord = rinfo.endRecord + 1;
				db.endRecord = rinfo.endRecord + 1;
				db.offset = record.length() + splitTemp;
				splitTemp = 0;
				duplicate.add(db);

				if (TLB.pagesOccupied < TLB.pageCount) {
					TLB.fillPageTable(rinfo.pageNo + 1, rinfo.tableNo);
					fb.frameBuffer.elementAt(TLB.pagesOccupied - 1).add(record);

				} else {
					int frame = TLB.timestamp.getLast();
					TLB.updatePageTable(frame, rinfo.pageNo + 1, rinfo.tableNo);
					fb.frameBuffer.elementAt(frame).clear();
					fb.frameBuffer.elementAt(frame).add(record);
				}
				rf.seek(rinfo.startLoc + rinfo.offset);
				rf.writeBytes(record);
			}

			rf.close();
		} catch (IOException e) {
			;
		}

		// TLB.printPageTable();
	}// end of insert function
	
	
	
	public static void main(String args[]) throws FileNotFoundException {

		NewQueryParser q = new NewQueryParser(args[0]);
		String query;
		DBSystem db = new DBSystem();
		q.setDbObject(db);
		db.readConfig(args[0]);
		db.populateDBInfo();
		//db.print();
		// Update this after done
		// System.out.print(args[1]);
		
		try {
			Scanner scanner = new Scanner(System.in);
			int testCases=Integer.parseInt(scanner.nextLine());
			

			while (testCases>0) {
				query = scanner.nextLine();
				q.queryType(query);
				q.columnList.clear();
				q.groupByColumnList.clear();
				q.conditionList.clear();
				q.selectColumnList.clear();
				q.orderByColumnList.clear();
				q.result.setLength(0);
				q.conditionOperator = "NA";
				q.tableList.clear();
				System.out.println();
				testCases--;
			}
			scanner.close();
		} catch (Exception e) {

		}
	}

	// */
}// end of DBSystem Class

/**
 * 
 * @author dhruvil and kapil
 * 
 */
class DBinfo {
	int strtRecord, endRecord, offset, split;
	long startingByte;

	public DBinfo() {
		split = 0;
		strtRecord = endRecord = -1;
	}
}


 /*
  * 
  */
class RecordInfo {
	int tableNo, pageNo, offset, strtRecord, endRecord, split;
	long startLoc;

	public RecordInfo() {
		strtRecord = endRecord = tableNo = pageNo = offset = -1;
		startLoc = -1;
		split = 0;
	}
}
