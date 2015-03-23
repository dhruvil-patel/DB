package execute;

import gudusoft.gsqlparser.nodes.TExpression;

public class JoinCondition {

	
	String leftOperator,rightOperator;
	public String leftTable;
	public String rightTable;
	
	public JoinCondition(TExpression tExpression) {
		String tmp[] = tExpression.getLeftOperand().toString().split("\\.");
		leftTable = tmp[0];
		leftOperator = tmp[1];
		tmp = tExpression.getRightOperand().toString().split("\\.");
		rightTable = tmp[0];
		rightOperator = tmp[1];
	}
}
