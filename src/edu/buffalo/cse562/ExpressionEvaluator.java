package edu.buffalo.cse562;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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

public class ExpressionEvaluator<T> implements SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor {
	
	private String leftCondition="";
	private Object value;
	private String rightCondition = "";
	public boolean result = true;
	private Table useTable;
	private boolean leftCondnBool = false;
	
	public Tuple input;

	public boolean isResult() {
		
		return result;
	}
	public void resetFlag()
	{
		result = true;
	}
	
	public boolean getOutput(Expression whereExpr,Table t,Tuple a)
	{
		useTable = t;
		input = a;
		//System.out.println(whereExpr+" "+t);
		whereExpr.accept(this);
		
		return result;
	}
	
	public ExpressionEvaluator() {
		// TODO Auto-generated constructor stub
	}
	
	ExpressionEvaluator(Tuple input)
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
		String rightExp = addition.getRightExpression().toString();
		String leftExp = addition.getLeftExpression().toString();
		/*if(isNumeric(rightExp) && isNumeric(leftExp))
		{
			long left = Integer.parseInt(leftExp);
			long right = Integer.parseInt(rightExp);
			
			value = left+right;
			
			//System.out.println("ADD "+value);
		}*/
		/*if(isDecimal(rightExp) && isDecimal(leftExp))
		{
			double left = Double.parseDouble(leftExp);
			double right = Double.parseDouble(rightExp);
			
			value = left+right;
			value =  Math.round((double)value*100.0)/100.0;
			//System.out.println(value);
		}*/
		if((rightExp.contains(".") && leftExp.contains(".")))
		{
			double left = Double.parseDouble(leftExp);
			double right = Double.parseDouble(rightExp);
			
			value = left+right;
			value =  Math.round((double)value*100.0)/100.0;
			//System.out.println(value);
		}
		else
		{
			long left = Integer.parseInt(leftExp);
			long right = Integer.parseInt(rightExp);
			
			value = left+right;
		}
		//visitBinaryExpression(addition);
	}

	public void visit(AndExpression andExpression) {
		
		//System.out.println(andExpression);
		value=null;
		
		andExpression.getLeftExpression().accept(this);
		//System.out.println("AND "+result);
		if(result){
		andExpression.getRightExpression().accept(this);
		}
	}

	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	public void visit(Column tableColumn) {
		/*
		System.out.println(tableColumn.toString());
		for(Entry<String,Datum> a : input.entrySet())
			System.out.print(a.getKey()+" "+a.getValue().getValue()+"|");
		System.out.println();*/
		if(tableColumn.toString().contains("."))
			value = input.get(tableColumn.toString()).getValue();
		else
		{
			/*String check="";
			Set<String> keys = input.keySet();
			for(String key : keys)
			{
				String temp[] = key.split("\\.");
				check = temp[0];
				break;
			}*/
			String check = useTable.getWholeTableName()+"."+tableColumn.toString();
			value = input.get(check).getValue();
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
		Datum data = null ;
		if(leftCondition.contains("."))
		{
			data = input.get(leftCondition);
		}
		else
		{
			String check = useTable.getWholeTableName()+"."+leftCondition;
			data = input.get(check);
			
		}
	//	System.out.println(data.getValue()+" "+value);
		if(data.getType().equals("int"))
		{
			//System.out.println(condition+" "+data.getValue()+" "+value);
			if(value != null)
			{	
				if ( (int)data.getValue() == (int)value)
					result &= true;
				else
					result = false;
				value = null;
				//System.out.println("int = "+result);
			}
			else
			{
				Datum data1 = input.get(rightCondition);
				//System.out.println(rightCondition);
				//System.out.println(condition+" "+data1.getValue()+" "+data.getValue());
				if((int)data.getValue() == (int)data1.getValue())
					result &= true;
				else
					result = false;
			}
		}
		if(data.getType().equals("double"))
		{
			if ( (double)data.getValue() == (double)value)
				result &= true;
			else
				result = false;
		}
		if(data.getType().equals("string"))
		{
			//System.out.println(condition+" "+value);
			if(value != null)
			{	
				//System.out.println(data.getValue()+" "+value);
				if ( ((String)data.getValue()).equals((String)value))
				{
					result = true;
				}
				else
					result = false;
			}
			else
			{
				
				Datum data1 = input.get(rightCondition);
				if(data.getValue().equals(data1.getValue()))
				{
				//	System.out.println("equals");
					result &= true;
				}
				else
					result = false;
			}
		}
		if(data.getType().equalsIgnoreCase("date"))
		{
			Date from =  (Date) data.getValue();
			if(((Date) value).compareTo(from) == 0){
        		result &= true;
			}
			else
				result = false;

		}
	}

	public void visit(Function function) {
		DateValue date = new DateValue(function.getParameters().toString().replaceAll("\\)|\\(", ""));
		//System.out.println(date.getValue());
		date.accept(this);
	}

	public void visit(GreaterThan greaterThan) {
		visitBinaryExpression(greaterThan);
		Datum data = null ;
		if(leftCondition.contains("."))
		{
			data = input.get(leftCondition);
		}
		else
		{
			if(leftCondnBool)
			{
				//String check = useTable.getWholeTableName()+"."+leftCondition;
				//System.out.println(check);
				data = input.get(leftCondition);
			}
			else
			{
				String check = useTable.getWholeTableName()+"."+leftCondition;
				//System.out.println(check);
				data = input.get(check);
			}
			leftCondnBool = false;
			
		}
		if(data.getType().equals("int"))
		{
			if ( (int)data.getValue() > (int)value)
			{
				result &= true;
			}
			else
				result = false;
		}
		if(data.getType().equals("double"))
		{
			if ( (double)data.getValue() > (double)value)
				result &= true;
			else
				result = false;
		}
		if(data.getType().equals("date"))
		{
			if(data.compareTo(new GetDate(value.toString())) == 1)
				result &= true;
			else
				result = false;
			/*Date from =  (Date) data.getValue();
			SimpleDateFormat out=new SimpleDateFormat("yyyy-MM-dd");
			String from1 = out.format(from);
			try {
				Date date1 = out.parse(value.toString());
				Date date2=out.parse(from1);
				if(date2.after(date1)){
	        		result &= true;
				}
				else
					result = false;
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		}
		
	}

	public void visit(GreaterThanEquals greaterThanEquals) {
		
		visitBinaryExpression(greaterThanEquals);
		Datum data = null ;
		if(leftCondition.contains("."))
		{
			data = input.get(leftCondition);
		}
		else
		{
			String check = useTable.getWholeTableName()+"."+leftCondition;
			data = input.get(check);
		}
		if(data.getType().equals("int"))
		{
			if ( (int)data.getValue() >= (int)value)
			{
			//	System.out.println((int)data.getValue()+" "+(long)value);
				result &= true;
			}
			else
			{
				result = false;
			}
		}
		else if(data.getType().equals("double"))
		{
			if ( (double)data.getValue() >= (double)value)
				result &= true;
			else
			{
				result = false;
			}
		}
		else if(data.getType().equals("date"))
		{
			if(data.compareTo(new GetDate(value.toString())) >= 0)
				result &= true;
			else
				result = false;
			/*Date from =  (Date) data.getValue();
			//System.out.println(" GTE "+value+" "+from);
			if(((Date) value).compareTo(from)<=0){
				//System.out.println(" GTE "+value+" "+from);
        		result &= true;
			}
			else
			{
				result = false;
			}*/
		}
		
		
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
		value = (int)longValue.getValue();
		//System.out.println(value);
	}

	public void visit(MinorThan minorThan) {
		
		visitBinaryExpression(minorThan);
		Datum data = null ;
		//System.out.println(condition);
		if(leftCondition.contains("."))
		{
			data = input.get(leftCondition);
		}
		else
		{
			String check = useTable.getWholeTableName()+"."+leftCondition;
			data = input.get(check);
		}
		//System.out.println(data.getType()+" "+data.getValue()+" "+value.toString());
			if(data.getType().equals("int"))
			{
				if ( (int)data.getValue() < (int)value)
				{
					//System.out.println((int)data.getValue()+" "+(long)value);
					result &= true;
				}
				else
				{
					result = false;
				}
			}
			else if(data.getType().equals("double"))
			{
				
				if ( (double)data.getValue() < (double)Double.parseDouble(value.toString()))
				{
					//System.out.println(data.getValue()+" "+value);
					result &= true;
				}
				else
					result = false;
			}
			else if(data.getType().equals("date"))
			{
				
				if(data.compareTo(new GetDate(value.toString())) == -1)
					result &= true;
				else
					result = false;
				/*Date from =  (Date) data.getValue();
				SimpleDateFormat out=new SimpleDateFormat("yyyy-MM-dd");
				String from1 = out.format(from);
				try {
					//System.out.println(value);
					Date date1 = out.parse(value.toString());
					Date date2=  out.parse(from1);
					if(date2.before(date1)){
		        		result &= true;
					}
					else
						result = false;
				} catch (java.text.ParseException e) {
					// TODO Auto-generated catch block
					try {
						Date date1 = out.parse(out.format((Date)value));
						Date date2 = out.parse(from1);
						if(date2.before(date1)){
			        		result &= true;
						}
						else
							result = false;
					} catch (java.text.ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}*/
					
					
				}
	}

	public void visit(MinorThanEquals minorThanEquals) {
		visitBinaryExpression(minorThanEquals);
		Datum data = null ;
		if(leftCondition.contains("."))
		{
			data = input.get(leftCondition);
		}
		else
		{
			String check = useTable.getWholeTableName()+"."+leftCondition;
			data = input.get(check);
		}
		if(data.getType().equals("int"))
		{
			if ( (int)data.getValue() <= (int)value)
				result &= true;
			else
				result = false;
		}
		
		if(data.getType().equals("double"))
		{
			if ( (double)data.getValue() <= (double)value)
				result &= true;
			else
				result = false;
		}
		else if(data.getType().equals("date"))
		{
			if(data.compareTo(new GetDate(value.toString())) <= 0)
				result &= true;
			else
				result = false;
			/*
			Date from =  (Date) data.getValue();
			SimpleDateFormat out=new SimpleDateFormat("yyyy-MM-dd");
			String from1 = out.format(from);
			
			try {
				Date date1 = out.parse(value.toString());
				Date date2=out.parse(from1);
				if(date2.before(date1)){
	        		result &= true;
				}
				else
					result = false;
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
*/			

		}
	}

	public void visit(Multiplication multiplication) {
		visitBinaryExpression(multiplication);
	}

	public void visit(NotEqualsTo notEqualsTo) {
		visitBinaryExpression(notEqualsTo);
		//visitBinaryExpression(minorThanEquals);
		Datum data = null ;
		if(leftCondition.contains("."))
		{
			data = input.get(leftCondition);
		}
		else
		{
			String check = useTable.getWholeTableName()+"."+leftCondition;
			data = input.get(check);
		}
		if(data.getType().equals("int"))
		{
			if ( (int)data.getValue() != (int)value)
				result &= true;
			else
				result = false;
		}
		if(data.getType().equals("double"))
		{
			if ( (double)data.getValue() != (double)value)
				result &= true;
			else
				result = false;
		}
		if(data.getType().equals("string"))
		{
			if (!data.getValue().equals(value))
				result &= true;
			else
				result = false;
		}
	}

	public void visit(NullValue nullValue) {
	}

	public void visit(OrExpression orExpression) {
		value=null;
		//System.out.println(orExpression);
		orExpression.getLeftExpression().accept(this);
		//System.out.println(result);
		if(!result){
		orExpression.getRightExpression().accept(this);
		}
		
	}

	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	public void visit(StringValue stringValue) {
		value = stringValue.getValue();
	}

	public void visit(Subtraction subtraction) {
		//System.out.println("Dasdad "+subtraction.getLeftExpression().toString());
		//System.out.println(subtraction);
		String leftExp = subtraction.getLeftExpression().toString();
		String rightExp = subtraction.getRightExpression().toString();
		int left = 0,right = 0;
		if(isDecimal(leftExp) && isDecimal(rightExp))
		{
			double ltemp = Double.parseDouble(leftExp);
			double rtemp = Double.parseDouble(rightExp);
			value = ltemp-rtemp;
			value =  Math.round((double)value*100.0)/100.0;
		}
		else
		{
			
			if(subtraction.getLeftExpression().toString().contains("."))
			{
				left = (int)input.get(leftExp).getValue();
				right = (int)input.get(rightExp).getValue();
				input.put(subtraction.toString(),new GetInteger((left-right)+""));
			}
			else
			{ 	
				leftCondnBool = true;
				String check = useTable.getWholeTableName()+"."+leftExp;
				left = (int)input.get(check).getValue();
				check = useTable.getWholeTableName()+"."+rightExp;
				left = (int)input.get(check).getValue();		
				
				input.put(subtraction.toString(),new GetInteger((left-right)+""));
			}
		}
		
		//visitBinaryExpression(subtraction);
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		//System.out.println(binaryExpression.getLeftExpression().toString());
		//System.out.println(binaryExpression.getRightExpression().toString());
		leftCondition = binaryExpression.getLeftExpression().toString();
		String rightExp = binaryExpression.getRightExpression().toString();
		/*String leftExp = binaryExpression.getLeftExpression().toString();
		if(isNumeric(rightExp) && isNumeric(leftExp))
		{
			double left = Double.parseDouble(leftExp);
			double right = Double.parseDouble(rightExp);
			
			value = left-right;
		}
		else*/
			rightCondition = rightExp;
		//System.out.println("RIGHT "+rightCondition);
		value=null;
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
		value = dateValue.getValue();
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