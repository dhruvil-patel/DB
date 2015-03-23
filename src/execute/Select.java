package execute;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.Vector;

import lru.*;
import parser.*;

/**
 * Executes Select Command.
 * 
 * @author dhruvil and kapil
 * 
 */

public class Select {

	public static Vector<Integer> OrderbyColumnNumbers = new Vector<Integer>(); 
	
	public static LinkedHashMap<String, String> pair;
	public static Vector<String> columnList = new Vector<String>();

	public Vector<Integer> SelectColumnNumbers = new Vector<Integer>();
	public PriorityQueue<Pack> tuples;
	public DBSystem db;
	public NewQueryParser command;
	public String table;
	int actualMemory;
	int memoryPageSize;
	int fileNumber;

	public void setSelectColumnNumbers(Vector<String> all, Vector<String> select) {

		// System.out.println("All"+all);
		// System.out.println("Select :"+select);
		int k = select.size();
		for (int i = 0; i < k; i++) {
			// System.out.println("i "+i);
			int index = all.indexOf(select.get(i));
			// System.out.println("index "+index);
			SelectColumnNumbers.add(index);
		}
		// System.out.print("Select Order :\n");
		// System.out.println(SelectColumnNumbers);
	}

	public void setDbObject(DBSystem d) {
		db = d;
		actualMemory = db.pageSize * db.noOfPages;
		memoryPageSize = db.pageSize;
	}

	public void setPairObject(LinkedHashMap<String, String> p) {
		Select.pair = p;

		// Set all Column list
		if (Select.columnList.size() > 0)
			Select.columnList.clear();

		for (String column : p.keySet()) {
			Select.columnList.add(column);
		}

	}

	public void setOrderByColumnNumbers(Vector<String> all,
			Vector<String> select) {
		if (Select.OrderbyColumnNumbers.size() > 0)
			Select.OrderbyColumnNumbers.clear();

		int k = select.size();
		for (int i = 0; i < k; i++) {
			int index = all.indexOf(select.get(i));
			Select.OrderbyColumnNumbers.add(index);
		}

	}

	/**
	 * 
	 */
	public void OrderBy(Vector<Integer> records) {

		//System.out.println("welCome to Here:");
		
		tuples = new PriorityQueue<Pack>(4, new SortTuples());
		int recordCount = records.size();
		String record;

		int fileNumber = 0;
		int memorySize = 0;

		// System.out.println(recordCount);
		for (int i = 0; i < recordCount; i++) {
			record = db.getRecord(table, records.get(i));
			
			//System.out.println(record);

			if (memorySize + record.length() < actualMemory) {
				memorySize += record.length();
				tuples.add(new Pack(record, fileNumber));
			} else {
				// System.out.println("Create File");
				try {
					File f = new File(db.filePath + "/intermediate"
							+ fileNumber);
					BufferedWriter writer = new BufferedWriter(new FileWriter(
							f, true));
					Pack p;
					while (!tuples.isEmpty()) {
						p = tuples.poll();
						writer.append(p.s);
						writer.newLine();
					}
					tuples.clear();
					writer.close();

					fileNumber++;
					memorySize = record.length();
					tuples.add(new Pack(record, fileNumber));

				} catch (Exception e) {

				}

			}
		}

		// Still Missing
		/**
		 * Print the name of columns in first Line.
		 */
		System.out.println("\n\n\n\n\n\n");
		if (fileNumber == 0) {
			/**
			 * Print directly to output
			 */
			// System.out.println("Print out");
			String raw;
			String recordSplit[];
			Pack p;
			int k = SelectColumnNumbers.size();
			while (!tuples.isEmpty()) {

				p = tuples.poll();
				raw = p.s;
				recordSplit = raw.split(",");
				k = SelectColumnNumbers.size();
				// System.out.println(k);
				for (int j = 0; j < k; j++) {
					System.out.print(recordSplit[SelectColumnNumbers
							.elementAt(j)]);
					// System.out.println(SelectColumnNumbers.elementAt(j));
					if (j + 1 < k)
						System.out.print(",");
				}
				System.out.println();

			}

			return;
		} else {
			try {
				File f = new File(db.fileName + "/intermediate" + fileNumber);
				BufferedWriter writer = new BufferedWriter(new FileWriter(f,
						true));
				Pack p;
				while (!tuples.isEmpty()) {
					p = tuples.poll();
					writer.append(p.s);
					writer.newLine();
				}
				tuples.clear();
				writer.close();

				fileNumber++;

			} catch (Exception e) {

			}

		}

		/**
		 * For the first read of intermediate files in memory.
		 */

		tuples.clear();
		File f[] = new File[fileNumber];
		BufferedReader br[] = new BufferedReader[fileNumber];
		int fileOpen[] = new int[fileNumber];
		int openCount = fileNumber;
		String line;

		for (int i = 0; i < fileNumber; i++) {

			try {
				int memory = 0;
				f[i] = new File(db.filePath + "/intermediate" + i);
				br[i] = new BufferedReader(new FileReader(f[i]));
				line = br[i].readLine();
				while (line != null) {

					if (line.length() + memory < (memoryPageSize) * 0.8) {
						memory += line.length();
						tuples.add(new Pack(line, i));

					} else {
						tuples.add(new Pack(line, i));
						break;
					}
					line = br[i].readLine();
				}
				if (line == null) {
					openCount--;
					fileOpen[i] = 1;
				}

			} catch (Exception e) {

			}
		}

		/**
		 * Now Merge and Output.
		 */

		Pack p;

		int k = SelectColumnNumbers.size();
		while (openCount > 0) {

			if (!tuples.isEmpty()) {
				p = tuples.poll();
				output(p.s, k);
				if (fileOpen[p.f] == 1)
					continue;
				else {
					try {
						line = br[p.f].readLine();
						if (line == null) {
							openCount--;
							br[p.f].close();
							f[p.f].delete();
							fileOpen[p.f] = 1;
						} else
							tuples.add(new Pack(line, p.f));
					} catch (Exception e) {

					}
				}
			}
		}

	}

	/**
	 * 
	 * Prints records if No OrderBy is required.
	 * 
	 * @param records
	 *            : list of records to output for query.
	 * @param tableName
	 */

	public void printTuples(Vector<Integer> records, String tableName) {

		/**
		 * It expects Select.SelectColumnNumbers to hold column numbers to be
		 * printed.
		 */

		String rawTuple;
		int l = records.size();
		int k = SelectColumnNumbers.size();
		int i = 0;

		while (i < l) {
			// System.out.println(records.get(i));
			// System.out.println(tableName);
			rawTuple = db.getRecord(tableName, records.get(i));

			output(rawTuple, k);
			i++;
		}

	}

	/**
	 * Prints each row to terminal.
	 * 
	 * @param rawTuple
	 *            : actual row from table
	 * @param k
	 *            : number of select colums
	 */

	public void output(String rawTuple, int k) {

		String rawSplit[] = rawTuple.split(",");

		for (int j = 0; j < k; j++) {
			System.out.print(rawSplit[SelectColumnNumbers.elementAt(j)]);
			if (j + 1 < k)
				System.out.print(",");
		}
		System.out.println();

	}

}

class SortTuples implements Comparator<Pack> {

	String firstSplit[];
	String secondSplit[];

	/**
	 * It expects Select.OrderByColumnNumbers to hold column numbers on which to
	 * sort.
	 */

	int l = Select.OrderbyColumnNumbers.size();
	boolean isInt;
	boolean isFloat;
	boolean isString;

	@Override
	public int compare(Pack first, Pack second) {
		// TODO Auto-generated method stub
		// System.out.println(first.s);
		firstSplit = first.s.split(",");
		secondSplit = second.s.split(",");

		int i = 0;
		while (i < l) {

			isInt = false;
			isFloat = false;
			isString = false;	
			/**
			 * You have to get list of names of orderby columns also.
			 */

			String temp;
			temp = firstSplit[Select.OrderbyColumnNumbers.get(i)];
			int l = temp.length();
			if (temp.charAt(0) == '\'' || temp.charAt(l - 1) == '\"') {
				temp = temp.substring(1, l - 1);
				firstSplit[Select.OrderbyColumnNumbers.get(i)] = temp;
			}

			temp = secondSplit[Select.OrderbyColumnNumbers.get(i)];
			l = temp.length();
			if (temp.charAt(0) == '\'' || temp.charAt(l - 1) == '\"') {
				temp = temp.substring(1, l - 1);
				secondSplit[Select.OrderbyColumnNumbers.get(i)] = temp;
			}

			// System.out.println(Select.pair);
			 String dataType = Select.pair.get(Select.columnList
					.get(Select.OrderbyColumnNumbers.elementAt(i)));

			 //System.out.println("Data Type : "+ dataType);

			 if (dataType.equalsIgnoreCase("integer")) {
				
				isInt = true;
				isFloat = false;
				isString = false;
			}
			else if (dataType.equalsIgnoreCase("float")) {
				
				isFloat = true;
				isInt = false;
				isString = false;
			}
			else  {
				
				isString = true;
				isInt = false;
				isFloat = false;
				
			}
				
			// System.out.println(firstSplit[Select.OrderbyColumnNumbers.get(i)]);

			if (isInt) {

				int a = Integer.parseInt(firstSplit[Select.OrderbyColumnNumbers
						.get(i)]);
				int b = Integer
						.parseInt(secondSplit[Select.OrderbyColumnNumbers
								.get(i)]);
				if (a == b) {
					i++;
					continue;
				} else if (a < b)
					return -1;
				else
					return 1;

			} 
			if (isFloat) {
				float a = Float
						.parseFloat(firstSplit[Select.OrderbyColumnNumbers
								.get(i)]);
				float b = Float
						.parseFloat(secondSplit[Select.OrderbyColumnNumbers
								.get(i)]);
				if (a == b) {
					i++;
					continue;
				} else if (a < b)
					return -1;
				else
					return 1;

			} 
			if(isString) {
				
				//System.out.println("\ncompare\n");
				int c = firstSplit[Select.OrderbyColumnNumbers.get(i)]
						.compareTo(secondSplit[Select.OrderbyColumnNumbers
								.get(i)]);
				if (c == 0) {
					i++;
				} else
					return c;
			}
		}
		return 1;
	}
}

class Pack implements Comparable {
	public String s;
	public int f;

	public Pack(String a, int id) {
		this.s = a;
		f = id;
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}

}
