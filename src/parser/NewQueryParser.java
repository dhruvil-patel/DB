package parser;

import lru.*;

import execute.Select;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TColumnDefinition;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.nodes.TOrderBy;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.Vector;

/**
 * 
 * @author dhruvil and kapil
 * 
 */

public class NewQueryParser {

	private TGSqlParser sqlParser;
	private TCreateTableSqlStatement parsedCreateTableStatement;
	private TSelectSqlStatement parsedSelectStatement;
	public StringBuilder result = new StringBuilder();
	public TreeMap<String, LinkedHashMap<String, String>> tableData = new TreeMap<String, LinkedHashMap<String, String>>();
	public LinkedHashMap<String, String> pair;

	public Vector<String> tableList = new Vector<String>(); // name of tables
	public String conditionOperator = "NA";
	public DBSystem db;
	public Vector<String> groupByColumnList = new Vector<String>();
	public Vector<String> orderByColumnList = new Vector<String>();
	public Vector<String> conditionList = new Vector<String>();
	public Vector<String> columnList = new Vector<String>();
	public Vector<String> selectColumnList = new Vector<String>();
	public String tName;
	public String cName;
	public String dataType;
	private String dataPath;
	private String configPath;

	public NewQueryParser(String configPath) {
		this.dataPath = "";
		this.configPath = configPath;
		EDbVendor dbVendor = EDbVendor.dbvmysql;
		sqlParser = new TGSqlParser(dbVendor);

		// setting dataPath

		String line;
		try {
			File configFile = new File(configPath);

			BufferedReader configReader = new BufferedReader(new FileReader(
					configFile));
			line = configReader.readLine();

			// read until PATH_FOR_DATA is found in config file
			while (line != null) {
				if (line.startsWith("PATH_FOR_DATA")) {
					this.dataPath = line.substring(14);
					break;
				}
				line = configReader.readLine();
			}
			configReader.close();
		} catch (Exception e) {

		}

	}

	/**
	 * Checks query type and call corresponding command function.
	 * 
	 * @param query
	 *            : Input query
	 */

	public void queryType(String query) {

		// For removing space at starting
		int i = 0;
		while (query.charAt(i) == ' ' || query.charAt(i) == '\t')
			i++;

		if (query.contains(" ")) {
			if (query.regionMatches(true, i, "select", 0, 6)) {
				selectCommand(query);
			} else if (query.regionMatches(true, i, "create", 0, 6)) {
				createCommand(query);
			} else {
				System.out.println("Query Invalid");
			}
		} else {
			System.out.println("Query Invalid");
		}

	}
	public void setDbObject(DBSystem d){
		db=d;
	}
	
	/**
	 * Validates Select Query.
	 * 
	 * @param query
	 *            : Input Query.
	 * @return : result (variable of type String Builder) which contains parsed
	 *         select statement based on field and if query is invalid then it
	 *         contains string "Query Invalid"
	 */
	public void selectCommand(String query) {

		sqlParser.sqltext = query;
		int ret = sqlParser.parse();
		if (ret == 0) {
			parsedSelectStatement = (TSelectSqlStatement) sqlParser.sqlstatements
					.get(0);

			result.setLength(0);
			result.append("Querytype:select\n");
			result.append("Tablename:");

			for (int i = 0; i < parsedSelectStatement.tables.size(); i++) {
				String tableName = parsedSelectStatement.tables.getTable(i)
						.toString();
				if (checkTable(tableName)) {
					tableList.add(tableName);
					result.append(tableName);
					result.append(',');
				} else {
					System.out.println("Query Invalid");
					return;
				}
			}
			result.replace(result.length() - 1, result.length(), "\n");

			/*
			 * Parse column List between Select and from this part handle *
			 * case,and check whether column is from clause Table List or not
			 */

			result.append("Columns:");
			for (int i = 0; i < parsedSelectStatement.getResultColumnList()
					.size(); i++) {
				String column = parsedSelectStatement.getResultColumnList()
						.getResultColumn(i).toString();
				if (column.equals("*")) {
					for (String tableName : tableList) {
						pair = tableData.get(tableName);
						for (String columnName : pair.keySet()) {

							/*
							 * Distict column names only
							 */
							if (!selectColumnList.contains(columnName)) {

								selectColumnList.add(columnName);
								result.append(columnName);
								result.append(',');
							}
						}
					}
				} else if (checkColumn(column)) {
					if (!selectColumnList.contains(column)) { // distinct column Name
														// should show
						selectColumnList.add(column);
						result.append(column);
						result.append(',');
					}

				} else {
					System.out.println("Query Invalid");
					return;
				}
			}
			result.replace(result.length() - 1, result.length(), "\n");

			/*
			 * Check for Distinct
			 */
			result.append("Distinct:");

			if (parsedSelectStatement.getSelectDistinct() != null) {
				for (String column : selectColumnList) {
					result.append(column);
					result.append(',');
				}
				result.replace(result.length() - 1, result.length(), "\n");
			} else {
				result.append("NA");
				result.append('\n');
			}

			// System.out.println(result);
			/*
			 * Where parsing
			 */

			result.append("Condition:");
			if (parsedSelectStatement.getWhereClause() != null) {
				String WhereClause = parsedSelectStatement.getWhereClause()
						.getCondition().toString();
				conditionOperator="A";
				if (WhereClause.toLowerCase().contains(" and ")) {
					conditionOperator = "AND";
				} else if (WhereClause.toLowerCase().contains(" or ")) {
					conditionOperator = "OR";
				}
				String[] split = WhereClause
						.split("\\sAND\\s|\\sand\\s|\\sOR\\s|\\sor\\s|\\sOr\\s|\\sAnd\\s");

				// For third phase for LIKE we have to separate strings first

				for (String condition : split) {
					condition = condition.trim();
					// System.out.println(condition);
					if (checkCondition(condition)) {
						result.append(condition);
						result.append(',');
					} else {
						System.out.println("Query Invalid");
						return;
					}
				}

				result.replace(result.length() - 1, result.length(), "\n");
			} else {
				result.append("NA");
				result.append('\n');
			}

			/*
			 * order By Parsing
			 */
			result.append("Orderby:");
			if (parsedSelectStatement.getOrderbyClause() != null) {

				TOrderBy OrderByClause = parsedSelectStatement
						.getOrderbyClause();

				for (int i = 0; i < OrderByClause.getItems().size(); i++) {
					String OrderByElement = OrderByClause.getItems()
							.elementAt(i).toString();

					if (!checkColumn(OrderByElement)) { // orderBy element need
														// not necessary in
														// select phrase column
														// list
						System.out.println("Query Invalid");
						return;
					} else {
						result.append(OrderByElement);
						result.append(',');
					}
				}
				result.replace(result.length() - 1, result.length(), "\n");

			} else {
				result.append("NA");
				result.append('\n');
			}

			/*
			 * Group By and Having Parsing
			 */
			if (parsedSelectStatement.getGroupByClause() != null) {
				// System.out.println("In GroupBy");
				TGroupBy GroupByClause = parsedSelectStatement
						.getGroupByClause();
				// System.out.println(GroupByClause);
				if (!GroupByClause.toString().toLowerCase()
						.contains("group by")
						&& GroupByClause.toString().toLowerCase()
								.contains("having")) {
					result.setLength(0);
					result.append("Query Invalid");
					System.out.println("Query Invalid");
					return;
				}
				result.append("Groupby:");
				if (GroupByClause.toString().toLowerCase().contains("group by")) {
					for (int i = 0; i < GroupByClause.getItems().size(); i++) {
						String groupByElement = GroupByClause.getItems()
								.elementAt(i).toString();
						if (!checkInColumnList(groupByElement)) {
							result.setLength(0);
							result.append("Query Invalid");
							System.out.println("Query Invalid");
							return;
						} else {
							groupByColumnList.add(groupByElement);
							result.append(groupByElement);
							result.append(',');
						}
					}
					result.replace(result.length() - 1, result.length(), "\n");
				}

				/*
				 * Having Clause
				 */

				result.append("Having:");
				if (GroupByClause.toString().toLowerCase().contains("having")) {
					String havingClause = parsedSelectStatement
							.getGroupByClause().getHavingClause().toString();

					String[] split = havingClause
							.split("\\sAND\\s|\\sand\\s|\\sOR\\s|\\sor\\s|\\sOr\\s|\\sAnd\\s");
					boolean continueFlag;
					for (String condition : split) {

						continueFlag = false;
						condition = condition.trim();
						for (String columnInGroupByList : groupByColumnList) {
							if (condition.startsWith(columnInGroupByList)) {
								continueFlag = true;
								break;
							}
						}
						if (!continueFlag) {
							System.out.println("Query Invalid");
							return;
						}
						if (checkCondition(condition)) {
							result.append(condition);
							result.append(',');
						} else {
							System.out.println("Query Invalid");
							return;
						}
					}
					result.replace(result.length() - 1, result.length(), "\n");
				} else {
					result.append("NA");
					result.append('\n');
				}
				// result.append('\n');
			} else {
				result.append("Groupby:");
				result.append("NA");
				result.append('\n');
				result.append("Having:");
				result.append("NA");
				result.append('\n');
			}

			// System.out.print(result.toString());
			selectQueryExecution(result.toString());
		} else {
			System.out.println("Query Invalid");
		}

	}

	public void setColumnNameOfTable(String TableName){
		if(columnList.size()>0)
				columnList.clear();
		pair = tableData.get(TableName);
		for (String colName : pair.keySet()) {
			columnList.add(colName);
		}
	}
	public Vector<Integer> conditionProcessor(String condition, String tableName) {
		Vector<Integer> resultantVector = new Vector<Integer>();
		try {
			if (condition.contains("!=")||condition.contains("<>")) {
				String []split= new String[2];
				if(condition.contains("!=")){
					split = condition.split("!=");
				}
				else {
					split = condition.split("<>");
				}
				String column = split[0].trim();
				if (column.contains(".")) {
					column = column.substring(column.indexOf('.') + 1);
				}
				checkColumn(column);
				BufferedReader br = new BufferedReader(new FileReader(new File(
						dataPath + "/" + tableName + "_" + column)));
				String line = null;
				line = br.readLine();
				if (dataType.toLowerCase().startsWith("integer")) {
					Integer value = Integer.parseInt(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Integer colValue = Integer.parseInt(split1[0].trim());
						if (colValue != value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				} else if (dataType.toLowerCase().startsWith("float")) {
					Float value = Float.parseFloat(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Float colValue = Float.parseFloat(split1[0].trim());
						if (colValue != value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				}else if (dataType.toLowerCase().startsWith("varchar")) {
					
					String value = split[1].trim();
					value = value.substring(1, value.length() - 1);
					while (line != null) {
						String[] split1 = line.split(":");
						String colValue = split1[0].trim();
						
						if (!colValue.equals(value)) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						
						}
						line = br.readLine();
					}
				}
				
				br.close();
			
			} else if (condition.contains(">=")) {
				String[] split = condition.split(">=");
				String column = split[0].trim();
				if (column.contains(".")) {
					column = column.substring(column.indexOf('.') + 1);
				}
				checkColumn(column);
				BufferedReader br = new BufferedReader(new FileReader(new File(
						dataPath + "/" + tableName + "_" + column)));
				String line = null;
				line = br.readLine();
				if (dataType.toLowerCase().startsWith("integer")) {
					Integer value = Integer.parseInt(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Integer colValue = Integer.parseInt(split1[0].trim());
						if (colValue >= value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				} else if (dataType.toLowerCase().startsWith("float")) {
					Float value = Float.parseFloat(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Float colValue = Float.parseFloat(split1[0].trim());
						if (colValue >= value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				}

				br.close();
			} else if (condition.contains("<=")) {
				String[] split = condition.split("<=");
				String column = split[0].trim();
				if (column.contains(".")) {
					column = column.substring(column.indexOf('.') + 1);
				}
				checkColumn(column);
				BufferedReader br = new BufferedReader(new FileReader(new File(
						dataPath + "/" + tableName + "_" + column)));
				String line = null;
				line = br.readLine();
				if (dataType.toLowerCase().startsWith("integer")) {
					Integer value = Integer.parseInt(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Integer colValue = Integer.parseInt(split1[0].trim());
						if (colValue <= value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				} else if (dataType.toLowerCase().startsWith("float")) {
					Float value = Float.parseFloat(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Float colValue = Float.parseFloat(split1[0].trim());
						if (colValue <= value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				}

				br.close();
			} else if (condition.contains(">")) {
				String[] split = condition.split(">");
				String column = split[0].trim();
				if (column.contains(".")) {
					column = column.substring(column.indexOf('.') + 1);
				}
				checkColumn(column);
				BufferedReader br = new BufferedReader(new FileReader(new File(
						dataPath + "/" + tableName + "_" + column)));
				String line = null;
				line = br.readLine();
				if (dataType.toLowerCase().startsWith("integer")) {
					Integer value = Integer.parseInt(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Integer colValue = Integer.parseInt(split1[0].trim());
						if (colValue > value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				} else if (dataType.toLowerCase().startsWith("float")) {
					Float value = Float.parseFloat(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Float colValue = Float.parseFloat(split1[0].trim());
						if (colValue > value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				}

				br.close();
			} else if (condition.contains("<")) {
				String[] split = condition.split("<");
				String column = split[0].trim();
				if (column.contains(".")) {
					column = column.substring(column.indexOf('.') + 1);
				}
				checkColumn(column);
				BufferedReader br = new BufferedReader(new FileReader(new File(
						dataPath + "/" + tableName + "_" + column)));
				String line = null;
				line = br.readLine();
				if (dataType.toLowerCase().startsWith("integer")) {
					Integer value = Integer.parseInt(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Integer colValue = Integer.parseInt(split1[0].trim());
						if (colValue < value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				} else if (dataType.toLowerCase().startsWith("float")) {
					Float value = Float.parseFloat(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Float colValue = Float.parseFloat(split1[0].trim());
						if (colValue < value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
						}
						line = br.readLine();
					}
				}

				br.close();
			} else if (condition.toLowerCase().contains("like")) {
				String[] split = condition
						.split("\\slike\\s|\\sLike\\s|\\sLIKE\\s");
				String column = split[0].trim();
				if (column.contains(".")) {
					column = column.substring(column.indexOf('.') + 1);
				}
				checkColumn(column);
				BufferedReader br = new BufferedReader(new FileReader(new File(
						dataPath + "/" + tableName + "_" + column)));
				String line = null;
				line = br.readLine();
				String value = split[1].trim();
				value = value.substring(1, value.length() - 1);
				while (line != null) {
					String[] split1 = line.split(":");
					String colValue = split1[0].trim();
					if (colValue.equalsIgnoreCase(value)) {
						String[] split2 = split1[1].split(",");
						for (int i = 0; i < split2.length; i++) {
							resultantVector.add(Integer.parseInt(split2[i].trim()));
						}
					}
					line = br.readLine();

				}
				br.close();
			} else if (condition.contains("=")) {
				String[] split = condition.split("=");
				String column = split[0].trim();
				if (column.contains(".")) {
					column = column.substring(column.indexOf('.') + 1);
				}
				checkColumn(column);
				BufferedReader br = new BufferedReader(new FileReader(new File(
						dataPath + "/" + tableName + "_" + column)));
				String line = null;
				line = br.readLine();
				if (dataType.toLowerCase().startsWith("integer")) {
					Integer value = Integer.parseInt(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Integer colValue = Integer.parseInt(split1[0].trim());
						if (colValue == value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
							break;
						}
						line = br.readLine();
					}
				} else if (dataType.toLowerCase().startsWith("float")) {
					Float value = Float.parseFloat(split[1].trim());
					while (line != null) {
						String[] split1 = line.split(":");
						Float colValue = Float.parseFloat(split1[0].trim());
						if (colValue == value) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
							break;
						}
						line = br.readLine();
					}
				} else if (dataType.toLowerCase().startsWith("varchar")) {
					String value = split[1].trim();
					value = value.substring(1, value.length() - 1);
					while (line != null) {
						String[] split1 = line.split(":");
						String colValue = split1[0].trim();
						if (colValue.equals(value)) {
							String[] split2 = split1[1].split(",");
							for (int i = 0; i < split2.length; i++) {
								resultantVector.add(Integer.parseInt(split2[i].trim()));
							}
							break;
						}
						line = br.readLine();
					}
				}

				br.close();
			}
			
		} catch (Exception e) {

		}
		return resultantVector;
	}

	
	/**
	 * Validates Create Query and if correct creates .data and .csv file and add
	 * entry in config file.
	 * 
	 * @param query
	 */
	public void selectQueryExecution(String selectParsed) {
		
		try {
			if (selectParsed.equalsIgnoreCase("Query Invalid")) {
				return;
			} else {
				//System.out.print(selectParsed);

				String []lineBreaker=selectParsed.split("\n");
				String token;
				int l=0,lineBreakerSize=lineBreaker.length;
				while (l<lineBreakerSize) {
					token = lineBreaker[l];
					l++;
					 if (token.startsWith("Condition:")) {
						token = token.substring(token.indexOf(':') + 1);
						if (!token.startsWith("NA")) {
							String[] split = token.split(",");
							for (int i = 0; i < split.length; i++) {
								conditionList.add(split[i]);
							}
						}
					} else if (token.startsWith("Orderby:")) {
						token = token.substring(token.indexOf(':') + 1);
						if (!token.startsWith("NA")) {
							String[] split = token.split(",");
							for (int i = 0; i < split.length; i++) {
								orderByColumnList.add(split[i]);
							}
						}
					}
				}
				

				String tableName = tableList.firstElement();
				
				setColumnNameOfTable(tableName);
				
				Vector<Integer> result = new Vector<Integer>();
				Vector<Integer> temp;
				if (conditionOperator.equals("NA")) {
						String col=selectColumnList.firstElement();
						BufferedReader br = new BufferedReader(new FileReader(new File(
								dataPath + "/" + tableName + "_" + col)));
						String line = null;
						line = br.readLine();
						while(line!=null){
							String[] split1 = line.split(":");
							String[] split2 = split1[1].split(",");
								for (int i = 0; i < split2.length; i++) {
									result.add(Integer.parseInt(split2[i].trim()));
								}
							line = br.readLine();
						}
						br.close();
						
				} else if (conditionOperator.equals("A")) {
					for (String condition : conditionList) {
						temp = conditionProcessor(condition, tableName);
						for(Integer val:temp){
							if(!result.contains(val)){
								result.add(val);
							}
						}
					}
				} else if (conditionOperator.equals("AND")) {
					Boolean flag=false;
					for (String condition : conditionList) {
						temp = conditionProcessor(condition, tableName);
						if(flag==true)
							result.retainAll(temp);
						else{
							for(Integer val:temp){
									result.add(val);
							}
							flag=true;
						}
						
					}
				} else if (conditionOperator.equals("OR")) {
					for (String condition : conditionList) {
						temp = conditionProcessor(condition, tableName);
						for(Integer val:temp){
							if(!result.contains(val)){
								result.add(val);
							}
						}
					}
				}
				//System.out.println("result -----> " + result);
			    Collections.sort(result);
				Select ss=new Select();
				ss.setDbObject(db);
				/**
				 * Print column names
				 */
				int sLength = selectColumnList.size();
				for(int i=0;i<sLength;i++)
				{
					System.out.print("\""+selectColumnList.elementAt(i)+"\"");
					if(i+1<sLength)
						System.out.print(",");
					else
						System.out.println();
				}
				
				
				if(orderByColumnList.size()>0){
					ss.table=tableName;
					ss.setPairObject(tableData.get(tableName));
					ss.setSelectColumnNumbers(columnList,selectColumnList);
					ss.setOrderByColumnNumbers(columnList,orderByColumnList);
					System.out.println("Its here : ");
					ss.OrderBy(result);
					System.out.println("Its here : ");
					
				}
				else{
				
					//System.out.println(columnList);
					ss.setSelectColumnNumbers(columnList,selectColumnList);
					
					//System.out.println("Out");
					ss.printTuples(result, tableName);
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
		
	}

	public void createCommand(String query) {

		sqlParser.sqltext = query;
		int ret = sqlParser.parse();
		if (ret == 0) {

			parsedCreateTableStatement = (TCreateTableSqlStatement) sqlParser.sqlstatements
					.get(0);
			String tableName = parsedCreateTableStatement.getTargetTable()
					.toString();

			// Code for validating table existence
			File file = new File(dataPath + "/" + tableName + ".csv");

			// If table does not exist then do following
			if (!file.exists()) {

				try {
					// Create table.csv file
					BufferedWriter writer = new BufferedWriter(new FileWriter(
							file));
					writer.close();

					// file config
					File configFile = new File(configPath);
					BufferedWriter configWriter = new BufferedWriter(
							new FileWriter(configFile, true));

					// write BEGIN to configfile
					configWriter.append("BEGIN\n");

					// create table.data file
					file = new File(dataPath + "/" + tableName + ".data");
					writer = new BufferedWriter(new FileWriter(file));

					System.out.println("QueryType:create");
					System.out.println("Tablename:"
							+ parsedCreateTableStatement.getTargetTable()
									.toString());
					System.out.print("Attributes:");

					// Write table name to configfile
					configWriter.append(parsedCreateTableStatement
							.getTargetTable().toString() + "\n");

					TColumnDefinition column;
					String attributeName, attributeType;

					for (int i = 0; i < parsedCreateTableStatement
							.getColumnList().size(); i++) {

						column = parsedCreateTableStatement.getColumnList()
								.getColumn(i);

						attributeName = column.getColumnName().toString();
						writer.append(attributeName + ":");
						System.out.print(attributeName);

						attributeType = column.getDatatype().toString();
						writer.append(attributeType.toLowerCase());
						System.out.print(" " + attributeType);

						// write attribute name and its type to configfile
						configWriter.append(attributeName + ","
								+ attributeType.toLowerCase() + "\n");

						if (i < parsedCreateTableStatement.getColumnList()
								.size() - 1) {
							System.out.print(",");
							writer.append(",");
						} else
							System.out.println("");
					}

					// write END to configfile
					configWriter.append("END\n");

					configWriter.close();
					writer.close();
				} catch (Exception e) {

				}
			} else {
				System.out.println("Query Invalid");
			}
		} else {
			System.out.println("Query Invalid");
		}

	}

	/**
	 * 
	 * @param tableName
	 * @return true if table exist otherwise false
	 */
	public boolean checkTable(String tableName) {

		try {
			File file = new File(dataPath + "/" + tableName + ".data");
			if (!file.exists()) {
				return false;
			} else {
				if (tableData.containsKey(tableName))
					return true;
				BufferedReader br = new BufferedReader(new FileReader(file));
				String fileText = br.readLine();
				String[] entry = fileText.split(",");
				pair = new LinkedHashMap<String, String>();
				for (int i = 0; i < entry.length; i++) {
					String[] columnDataTypePair = entry[i].split(":");
					pair.put(columnDataTypePair[0], columnDataTypePair[1]);

				}
				tableData.put(tableName, pair);
				br.close();

			}

		} catch (IOException e) {
			// System.out.println("IO Exception: checkTable function");
		}
		return true;
	}

	/**
	 * 
	 * @param : tableName columnName
	 * @return : true if table contains that column otherwise false
	 */
	public boolean tableContainsColumn(String tableName, String columnName) {
		if (!tableList.contains(tableName))
			return false;

		pair = tableData.get(tableName);
		for (String colName : pair.keySet()) {
			if (colName.equals(columnName)) {
				dataType = pair.get(colName);
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * @param columnName
	 * @return true if columnName is exists from any one of table of From clause
	 *         otherwise false
	 */
	public boolean checkColumn(String columnName) {
		// /System.out.println(columnName);

		cName = columnName;
		if (columnName.contains(".")) {
			tName = columnName.substring(0, columnName.indexOf('.'));
			cName = columnName.substring(columnName.indexOf('.') + 1);
			return tableContainsColumn(tName, cName);
		}
		int match = 0;
		for (String tableName : tableList) {
			tName = tableName;
			pair = tableData.get(tableName);
			for (String colName : pair.keySet()) {
				if (colName.equals(columnName)) {
					dataType = pair.get(colName);
					match++;
				}
			}
		}
		if (match == 1)
			return true;
		else
			return false;
	}

	/**
	 * 
	 * @param columnName
	 * @return true if select phrase contains column otherwise false
	 */
	public boolean checkInColumnList(String columnName) {
		// System.out.println(columnName);
		for (String cName : selectColumnList) {
			if (cName.equals(columnName))
				return true;
		}
		return false;
	}

	/**
	 * 
	 * @param condition
	 * @return true if condition is true otherwise false
	 */
	public boolean checkCondition(String condition) {
		String[] split = condition.split("<>|<=|>=|>|<|=|LIKE|like|Like|!=");

		if (split.length != 2) {
			
			return false;
		}

		String token = split[0].trim();
		String value = split[1].trim();
		String dt = null;
		if (checkColumn(token)) {

			try {
				Integer.parseInt(value);
				dt = "integer";
			} catch (NumberFormatException e) {

			}
			if (dt == null) {
				try {
					Float.parseFloat(value);
					dt = "float";
				} catch (NumberFormatException e) {

				}

				if (dt == null) {
					if (value.startsWith("\"") || value.startsWith("'")) {
						dt = "varchar";

					} else {
						dt = dataType;

						if (checkColumn(value)) {
							String temp = dataType;
							dataType = dt;
							dt = temp;

						} else
							return false;

					}
				}
				// System.out.println(dataType+" "+dt);
			}

			if (!dataType.toLowerCase().startsWith(dt.toLowerCase())) {
				// System.out.println("Query Invalid");
				return false;
			}
		} else {
			// System.out.println("Query Invalid");
			return false;
		}

		return true;
	}


}