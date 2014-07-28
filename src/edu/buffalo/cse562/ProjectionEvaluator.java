package edu.buffalo.cse562;

import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
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
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class ProjectionEvaluator implements SelectItemVisitor, SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor {

	//private double rval,lval;
	Object value;
	protected Tuple input;
    String col,fullCol,tempCol;
    String t = null;
    
    public ProjectionEvaluator(){}

    
	@Override
	public void visit(AllColumns a) {
		
		
		
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllTableColumns arg0) {
	
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SelectExpressionItem a) {
	//	System.out.println(a.toString());
		if(a.getAlias() != null)
		{
			fullCol = a.toString();
			tempCol = a.getAlias();
			col=a.getAlias();
		}
		//System.out.println("EVA "+fullCol+" "+tempCol+" "+col);
		for(Entry<String, String> entry : Constants.tableAliasMap.entrySet())
		{
			if(a.toString().contains(entry.getKey()))
			{
				t = entry.getValue();
			}
		}
		a.getExpression().accept(this);
		
		
			//System.out.println(a.getAlias());
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(ExpressionList arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(LongValue arg0) {
		value = arg0.getValue();
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Multiplication a) {
		
		//System.out.println("NUL "+a);
		double rval,lval;
		a.getLeftExpression().accept(this);
		lval=(double) value;
		a.getRightExpression().accept(this);
	    rval=(double)value;	
	    value = lval * rval;
	    //System.out.println("Mul "+a+" "+value+" "+lval+" "+rval);
	   // input.put(a.toString(),new GetDouble(value+""));
		//input.put(t+"."+col.trim(),new GetDouble(value+""));
		input.put(col.trim(),new GetDouble(value+""));
		input.put(fullCol.trim(), new GetDouble(value+""));
		
	}
	@Override
	public void visit(Subtraction a) {
		double rval,lval;
		a.getLeftExpression().accept(this);
		lval=(long) value;
		a.getRightExpression().accept(this);
	    rval=(double)value;	
		value=lval-rval;
		//System.out.println("Subtraction "+a+" "+value);
	}
	
	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Column a) {
		//System.out.println(a.toString());
		
		if(a.toString().contains(".")) 
		{
			
			if(input.get(a.toString()).getType().equalsIgnoreCase("string"))
			{
				value = (String)input.get(a.toString()).getValue();
				input.put(t.trim()+"."+col.trim(),new GetString(value+""));
				input.put(fullCol.trim(),new GetString(value+""));
			}
			if(input.get(a.toString()).getType().equalsIgnoreCase("int"))
				value = (int)input.get(a.toString()).getValue(); 
			if(input.get(a.toString()).getType().equalsIgnoreCase("double"))
				value = (double)input.get(a.toString()).getValue(); 
			//System.out.println("visit column "+a+" "+value);
		}
		else
		{
			//System.out.println("inside else");
			String check=""; 
			Set<String> keys = input.keySet(); 
			for(String key : keys) 
			{ 
				String temp[] = key.split("\\."); 
				check = temp[0];
				break; 
			} 
			
			if((input.get(check+"."+a.toString()).getType().equalsIgnoreCase("int")))
					value=(int)input.get(check+"."+a.toString()).getValue();
		}
		
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
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
	@Override
	public void visit(Table arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(PlainSelect arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub
		
	}

}
