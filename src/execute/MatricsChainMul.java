package execute;

import gudusoft.gsqlparser.nodes.TExpression;

import java.util.HashMap;
import java.util.Vector;

import lru.DBSystem;


public class MatricsChainMul {
	
	float m[][];
    int s[][];
    Vector<String> joinTableList;
   
    public MatricsChainMul(Vector<JoinCondition> joinConditions,Vector<String> _joinTableList) {
        joinTableList = _joinTableList;
        int table_count;
        table_count = joinTableList.size();
        table_count++;
        m = new float[table_count][table_count];
        s = new int[table_count][table_count];
    
        
        
        for(int i = 1; i < table_count;i++){
            for(int j = 1; j < table_count; j++){

        		m[i][j] = 0;
            }
        }
        
        //TODO Comment following lines
        
        if(true){
	        System.out.println("V(o,cid)"+V("Orders","CustomerID"));
	        System.out.println("V(c,cid)"+V("Customers","CustomerID"));
	        System.out.println("V(i,iid)"+V("Items","ItemID"));
	        System.out.println("V(o,iid)"+V("Orders","ItemID"));
        }
        
        
        
        int count = table_count - 1;
        int a = 1;
        float temp = 0;
        
        while(count > 0){
            for(int i = 1; i < count; i++){
                int j = i + a;
                for(int k = i; k < j; k++){
                    temp = m [i][k] + m[k+1][j]
                    		+ ((( m [i][k] == 0 && i==k ? T(joinTableList.get(i-1)) : m[i][k])
                    				*( m[k+1][j] == 0 && k+1==j? T(joinTableList.get(k)) : m[k+1][j])
                    						/ Math.max(V(joinConditions.get(k-1).leftTable, joinConditions.get(k-1).leftOperator),V(joinConditions.get(k-1).rightTable, joinConditions.get(k-1).rightOperator)) ));//V[i-1] * V[k] * V[j]);
                    if(m[i][j] == 0 || m[i][j] > temp){
                        m[i][j] = temp;
                        s[i][j] = k;
                    }
                }
                
            }
                count--;
                a++;
        }
        
        if(true){
	        for(int i = 1; i < m.length;i++){
	        	for(int j=1; j < m[i].length;j++)
	        		System.out.print(m[i][j]+"\t");
	        	System.out.println();
	        }
	        for(int i = 1; i < s.length;i++){
	        	for(int j=1; j < s[i].length;j++)
	        		System.out.print((int)s[i][j]+"\t");
	        	System.out.println();
	        }
	        System.out.println();
	        
        }
        printChain(1, table_count-1, s);
        System.out.println();
    }    
    
    
    private int T(String table) {
    	
    	//TODO define T(R)
    	
    //	return (int) tables.get(table).lastRecord;
    	
    	return 0;
	}

    
    void printChain(int i, int j, int s[][]) {
    	   int k;
    	   if (i == j) {
    	      System.out.print(joinTableList.get(i-1));
    	      return;
    	   }
    	   System.out.print("(");
    	   k = s[i][j];
    	   printChain(i, k, s);
    	   System.out.print(",");
    	   printChain(k+1, j, s);
    	   System.out.print(")");
    	}
    
    int V(String tableName, String attributeName){
    	
    	//TODO define  V(R,A)
		//Table t = tables.get(tableName);
		//return t.index[t.attIndexList.get(attributeName)].size();
    	
    	return 0;
	}

}




