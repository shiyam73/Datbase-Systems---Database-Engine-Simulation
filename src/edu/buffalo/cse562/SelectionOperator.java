package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Date;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;

public class SelectionOperator<T> extends ExpressionEvaluator implements Operator {

	private Operator operator;
	private Expression conditionWhere;
	private Table table;
	
	
	public SelectionOperator() {
		// TODO Auto-generated constructor stub
		super();
	}
	
	public SelectionOperator(Operator operator,Expression condition,Table table)
	{
		this.operator = operator;
		this.conditionWhere = condition;
		this.table = table;
	}
	
	@Override
	public Tuple readOnetuple() {
		// TODO Auto-generated method stub
		int count = 0;
		
		
		do
		{
			input = operator.readOnetuple();
			if(input != null)
			{
				
				if(evaluate())
				{
					
					//System.out.println("In selection evaluator ");
					/*for (Map.Entry<String, Datum> entry : input.entrySet()) {
						System.out.print(entry.getKey() +"|"+ entry.getValue().getValue()+"  ");
					}
					System.out.println();*/
					count++;
					return input;
				}
				else
					resetFlag();
				
			}
			
		}while(input != null);
		return null;
	}
	
	public boolean evaluate()
	{
		return getOutput(conditionWhere, table,input);
		
		
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		operator.reset();
	}
	
	

}
