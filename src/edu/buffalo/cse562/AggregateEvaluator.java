package edu.buffalo.cse562;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class AggregateEvaluator implements SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor {
	
	private String condition="";
	
	private String rightCondition = "";
	protected String alias = null;
	Object value;
	double result;
	//ArrayList<Double> result = new ArrayList<Double>(); 
	
	public Tuple input;

	
	
	
	 AggregateEvaluator()
	 {
		 
	 }
	
	AggregateEvaluator(Tuple input)
	{
		this.input = input;
	}

	
	
	

	public void visit(PlainSelect plainSelect) {
		plainSelect.getFromItem().accept(this);
		
		if (plainSelect.getJoins() != null) {
			for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
				Join join = (Join) joinsIt.next();
				join.getRightItem().accept(this);
			}
		}
		if (plainSelect.getWhere() != null)
			plainSelect.getWhere().accept(this);

	}

	public void visit(Union union) {
		for (Iterator iter = union.getPlainSelects().iterator(); iter.hasNext();) {
			PlainSelect plainSelect = (PlainSelect) iter.next();
			visit(plainSelect);
		}
	}

	public void visit(Table tableName) {
		String tableWholeName = tableName.getWholeTableName();
		//tables.add(tableWholeName);
	}

	public void visit(SubSelect subSelect) {
		subSelect.getSelectBody().accept(this);
	}

	public void visit(Addition addition) {
		addition.getLeftExpression().accept(this);
		if(value instanceof Long)
		{
			long left = (long)value;
			addition.getRightExpression().accept(this);
			double right = (double)value;
			value = left + right;
			//System.out.println("ADD ::"+(left+right)+" "+left+" "+right);
		}
		else
		{
			double left = (double)value;
			addition.getRightExpression().accept(this);
			double right = (double)value;
			value = left + right;
		//	System.out.println("ADD ::"+(left+right)+" "+left+" "+right);
		}
	}

	public void visit(AndExpression andExpression) {
		//System.out.println("AND");
	
		visitBinaryExpression(andExpression);
	}

	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	public void visit(Column tableColumn) {
		
		String column=tableColumn.toString();
		//System.out.println("In column visitor 1:: "+column);
		String check="";
		Set<String> keys = input.keySet();
		for(String key : keys)
		{
			String temp[] = key.split("\\.");
			check = temp[0];
			break;
		}
		
		if(column.contains("."))
		{
			column = tableColumn.toString();
		}
		else
		{
			if(!column.contains(check))
			{
					column = check+"."+tableColumn.toString();
			}
			else
				column = tableColumn.toString();
		}
		
	//	System.out.println("In column visitor 2:: "+column);
		//System.out.println(column);
		try
		{
			value = (double)input.get(column).getValue();
		}
		catch(Exception e)
		{
			//System.out.println("In exception "+column);
		}
		
	}

	public void visit(Division division) {
		visitBinaryExpression(division);
	}

	public void visit(DoubleValue doubleValue) {
		value = doubleValue.getValue();
	}

	public void visit(EqualsTo equalsTo) {
		visitBinaryExpression(equalsTo);
	}

	public void visit(Function function) {
		//System.out.println(function.toString());
		DateValue date = new DateValue(function.getParameters().toString().replaceAll("\\)|\\(", ""));
		date.accept(this);
	}

	public void visit(GreaterThan greaterThan) {
		visitBinaryExpression(greaterThan);
	}

	public void visit(GreaterThanEquals greaterThanEquals) {
		visitBinaryExpression(greaterThanEquals);
	}
	
	

	public void visit(InExpression inExpression) {
		inExpression.getLeftExpression().accept(this);
		inExpression.getItemsList().accept(this);
	}

	public void visit(InverseExpression inverseExpression) {
		inverseExpression.getExpression().accept(this);
	}

	public void visit(IsNullExpression isNullExpression) {
	}

	public void visit(JdbcParameter jdbcParameter) {
	}

	public void visit(LikeExpression likeExpression) {
		visitBinaryExpression(likeExpression);
	}

	public void visit(ExistsExpression existsExpression) {
		existsExpression.getRightExpression().accept(this);
	}

	public void visit(LongValue longValue) {
		
		value = longValue.getValue();
	}

	public void visit(MinorThan minorThan) {
		visitBinaryExpression(minorThan);
	}

	public void visit(MinorThanEquals minorThanEquals) {
		visitBinaryExpression(minorThanEquals);
	}

	public void visit(Multiplication multiplication) {
		//System.out.println("in multiplication "+multiplication.getLeftExpression().toString());
		//System.out.println("mul "+multiplication.toString());
		multiplication.getLeftExpression().accept(this);
		double left = (double)value;
		multiplication.getRightExpression().accept(this);
		double right = (double)value;
		//result = left*right;
		value = left*right;
		//System.out.println("MUL ::"+value+" "+left+" "+right);
		//System.out.println("Alias :"+alias);
		input.put(multiplication.toString().replaceAll("\\(|\\)",""), new GetDouble(value+""));
		//input.put(alias, new GetDouble(value+""));
		//System.out.println(alias);
	}

	public void visit(NotEqualsTo notEqualsTo) {
		visitBinaryExpression(notEqualsTo);
	}

	public void visit(NullValue nullValue) {
	}

	public void visit(OrExpression orExpression) {
		visitBinaryExpression(orExpression);
	}

	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	public void visit(StringValue stringValue) {
		//value = stringValue.getValue();
	}

	public void visit(Subtraction subtraction) {
		//System.out.println("Dasdad "+subtraction.getLeftExpression().toString());
		//int left = 0,right = 0;
	
		//if(subtraction.getLeftExpression().toString().contains("\\.") || subtraction.getRightExpression().toString().contains("\\."))
		//{
		//	System.out.println("sub");
			subtraction.getLeftExpression().accept(this);
			if(value instanceof Long)
			{
				long left = (long)value;
				subtraction.getRightExpression().accept(this);
				double right = (double)value;
				value = left - right;
				//System.out.println("SUB ::"+(left-right)+" "+left+" "+right);
			}
			else
			{
				double left = (double)value;
				subtraction.getRightExpression().accept(this);
				double right = (double)value;
				value = left - right;
				//System.out.println("SUB ::"+(left-right)+" "+left+" "+right);
			}
			
			
			//input.put(subtraction.toString(),new GetDouble((left-right)+""));
		//}
		
		
		//visitBinaryExpression(subtraction);
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		//System.out.println(binaryExpression.getLeftExpression().toString());
		//System.out.println(binaryExpression.getRightExpression().toString());
		condition = binaryExpression.getLeftExpression().toString();
		if(isNumeric(binaryExpression.getRightExpression().toString()) && isNumeric(binaryExpression.getLeftExpression().toString()))
		{
			double left = Double.parseDouble(binaryExpression.getLeftExpression().toString());
			double right = Double.parseDouble(binaryExpression.getRightExpression().toString());
			
			value = left-right;
		}
		else
			rightCondition = binaryExpression.getRightExpression().toString();
		//System.out.println("RIGHT "+rightCondition);
		//value=null;
		binaryExpression.getLeftExpression().accept(this);
		binaryExpression.getRightExpression().accept(this);
	}

	public void visit(ExpressionList expressionList) {
		for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
			Expression expression = (Expression) iter.next();
			expression.accept(this);
		}

	}

	public void visit(DateValue dateValue) {
		//value = dateValue.getValue();
	}
	
	public boolean isNumeric(String str)
	{
	  return str.matches("-?\\d+(\\d+)?");  //match a number with optional '-' and decimal.
	}
	
	public boolean isDecimal(String str)
	{
	  return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
	}
	
	public void visit(TimestampValue timestampValue) {
	}
	
	public void visit(TimeValue timeValue) {
	}

	public void visit(CaseExpression caseExpression) {
	}

	public void visit(WhenClause whenClause) {
	}

	public void visit(AllComparisonExpression allComparisonExpression) {
		allComparisonExpression.getSubSelect().getSelectBody().accept(this);
	}

	public void visit(AnyComparisonExpression anyComparisonExpression) {
		anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
	}

	public void visit(SubJoin subjoin) {
		subjoin.getLeft().accept(this);
		subjoin.getJoin().getRightItem().accept(this);
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

}