package edu.buffalo.cse562;

import java.io.BufferedOutputStream;

import edu.buffalo.cse562.Constants;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

import javax.swing.plaf.multi.MultiButtonUI;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SelectEvaluator extends AbstractSelectEvaluator{

	private ArrayList<Column> cols = null;
	private ArrayList<Function> function = null;
	
	private String strroot = null;
	private String strprevroot = null;
	
	private String dataDir = null;
	private Map<Table, List<ColumnDefinition>> tables = null;
	private Operator root = null;
	private Operator prevroot = null;
	private ArrayList<String> whereconditions = new ArrayList<String>();
	private ArrayList<String> notEquiJoin = new ArrayList<String>();
	private ArrayList<String> notEquiJoinSelection = new ArrayList<String>();
	private ArrayList<String> forSelectionJoin = new ArrayList<String>();
	private Table aggregateTable = null;
	private List<SelectItem> selItems;
	private String rootTable = null;
	private String prevrootTable = null;
	private boolean bool = false;
	private String pushingProjection = null;
	private String[] indexScanConditions = null;
	private Expression onExpr = null;
	
	public SelectEvaluator(String dataDir,Map<Table, List<ColumnDefinition>> tables) 
	{
		// TODO Auto-generated constructor stub
		this.dataDir = dataDir;
		this.tables = tables;
	}

	public void visit(PlainSelect ps) {
		// TODO Auto-generated method stub
		int count=0;
		pushingProjection = ps.toString();
		
		if(ps.getWhere()!=null)
		{
			//System.out.println("1");
			String whereCondition = ps.getWhere().toString();
			Scanner sc = new Scanner(whereCondition);
			sc.useDelimiter("AND");

			while (sc.hasNext()) 
			{
				sc.useDelimiter("AND");
				String temp1 = sc.next();
				String temp[] = null;
				if(check(temp1))
				{
					temp = temp1.split("=");
					//System.out.println("ASAS "+temp[0]+" sdfs "+temp[1]);
					if(temp != null)
					{
						if(temp[0].contains(".") && temp[1].contains("."))
						{
							//System.out.println(temp1);
							whereconditions.add(temp1);
						}
						else
						{
						//	System.out.println(temp1);
							notEquiJoin.add(temp1.trim());
							notEquiJoinSelection.add(temp1.trim());
						}

					}
				}
				else
				{
				//	System.out.println(temp1);
					notEquiJoin.add(temp1.trim());
					notEquiJoinSelection.add(temp1.trim());
				}
			}
			sc.close();
		}
		
		for(int i=0;i<notEquiJoin.size();i++)
		{
			String condn = notEquiJoin.get(i);
			if(condn.contains("<>"))
			{
					forSelectionJoin.add(condn);
			}
			else if(condn.contains("<"))
			{
				String temp[] = condn.split("<");
				if(temp[0].contains(".") && temp[1].contains("."))
				{
					forSelectionJoin.add(condn);
				}
			}
		}
		
		
		for(int i=0;i<forSelectionJoin.size();i++)
		{
		//	System.out.println(forSelectionJoin.get(i));
			notEquiJoin.remove(forSelectionJoin.get(i));
		}
		
		for(int i=0;i<notEquiJoin.size();i++)
		{
			//System.out.println(notEquiJoin.get(i));
			
			String orCondn = notEquiJoin.get(i);
			if(orCondn.contains(" OR "))
			{
				notEquiJoin.remove(orCondn);
				String temp[] = orCondn.split(" OR ");
				for(int j=0;j<temp.length;j++)
				{
					notEquiJoin.add(temp[j].replaceAll("\\(|\\)", "").trim());
				}
			}
		}
		
		
		String date="";
		
		for(int i=0;i<notEquiJoin.size();i++)
		{
			String condn = notEquiJoin.get(i);
			//System.out.println(condn);
			if(condn.contains("date") && (condn.contains("date(") || condn.contains("DATE(")))
			{
				notEquiJoin.remove(condn);
				i=i-1;
				String temp[] = null;
				if(condn.contains(">="))
				{
					temp = condn.split(">=");
					if(!date.contains(temp[0]))
						date = temp[0].trim()+"|GTE|"+temp[1].replaceAll("date|\\(|\\)|\\'|DATE","").trim();
				}
				else if(condn.contains("<="))
				{
					temp = condn.split("<=");
						date += "|LTE|"+temp[1].replaceAll("date|\\(|\\)|\\'|DATE","").trim();
				}
				else if(condn.contains("<"))
				{
					temp = condn.split("<");
						date += "|LT|"+temp[1].replaceAll("date|\\(|\\)|\\'|DATE","").trim();
				}
				
			}
		}
		notEquiJoin.add(date);
		/*for(int i=0;i<notEquiJoin.size();i++)
		{
			String condn = notEquiJoin.get(i);
			//System.out.println(condn);
			if(condn.contains("date") && (condn.contains("date(") || condn.contains("DATE(")))
			{
				notEquiJoin.remove(condn);
				i=i-1;
			}
		}
		
		for(int i=0;i<notEquiJoin.size();i++)
		{
			String condn = notEquiJoin.get(i);
			System.out.println(condn);
		}*/
		
		//System.out.println("DATE "+date);
		
		/*for(int i=0;i<notEquiJoinSelection.size();i++)
		{
			String condn = notEquiJoinSelection.get(i);
			System.out.println("NEJS "+condn);
		}
		
		System.out.println();
		
		for(int i=0;i<forSelectionJoin.size();i++)
		{
			String condn = forSelectionJoin.get(i);
			System.out.println("FSJ "+condn);
		}*/
		
		
		if(ps.getFromItem()!=null)
		{
			ps.getFromItem().accept(this);
		}
		
		if(ps.getJoins()!=null)
		{
			
				List<Join> joins = ps.getJoins();
				for(Join j : joins)
				{
					j.getRightItem().accept(this);
					/*strroot = new String("JOIN OPERATOR:\n[\n\t"+ strroot + ",\n\t"+strprevroot+"\n]");*/
					//System.out.println("Inside join");
					String condition = getWhereCondition(prevrootTable,rootTable,whereconditions);
					//System.out.println(strroot);
					//	System.out.println(condition);
					if(condition != null)
					{
						root = new HashJoinOperator(prevroot,root,condition,prevrootTable,Constants.joinLevel);
						Constants.joinLevel++;
					}
					//root = new JoinOperator(prevroot, root);
				}
			
		
		}
		
		if(count==0)
		{
			
			if(ps.getWhere()!=null)
			{
				
				String temp = new String("SELECTION OPERATOR:("+ps.getWhere()+")[\n\t"+ strroot + "\n]");
				root = new SelectionOperator(root,onExpr,aggregateTable);
				strprevroot=strroot;
				strroot=temp;
				//System.out.println(strroot);
			}
		}
		if(ps.getSelectItems()!=null)
		{
			selItems = ps.getSelectItems();
			for(SelectItem si : selItems)
			{
				si.accept(this);
			}
			
			if(cols != null || function != null)
			{
				if(function != null)
				{
					List groupBy = ps.getGroupByColumnReferences();
					String temp = new String("AGGREGATE OPERATOR:("+cols+function+")[\n\t"+ strroot + "\n]");
					root = new AggregateOperator(root, ps, function, cols, groupBy,aggregateTable,selItems);
					strprevroot=strroot;
					strroot=temp;
					//System.out.println(strroot);
				}
				String temp = new String("PROJECTION OPERATOR:("+ps.getSelectItems()+")[\n\t"+ strroot + "\n]");
				//System.out.println(subtractionExp);
				root = new ProjectionOperator(root,selItems,aggregateTable);
				strprevroot=strroot;
				strroot=temp;
				//System.out.println(strroot);
			}
			
			
		}
		
		if(ps.getOrderByElements()!=null)
		{
			List ordElements = ps.getOrderByElements();
			for(Object si : ordElements)
			{
				//System.out.println(si.getClass());
			}
		}
		
	}
	
	
	
	/*public Expression getOnExpression(ArrayList<String> list)
	{
		Expression onExpr = null;
		StringBuilder tempExpr = new StringBuilder();
		//System.out.println(list.size());
		for (int i = 0; i < list.size(); i++) {					
			if (i == list.size() - 1) {
				tempExpr.append(list.get(i));
			} else {						
				tempExpr.append(list.get(i));
				tempExpr.append(" AND ");
			}
		}
		
		//System.out.println(tempExpr);
		StringReader strReader=new StringReader(tempExpr.toString());
		CCJSqlParser parser = new CCJSqlParser(strReader);
		try {
			onExpr=parser.Expression();
		} 
		catch (net.sf.jsqlparser.parser.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("OnExpr:: "+onExpr.toString());
		return onExpr;
	}*/
	
	public String getWhereCondition(String second,String first,ArrayList<String> list)
	{
		//System.out.println(first+ " "+second);
		
		String temp[] = null;
		String result = null;

		if(!bool)
		{
			for(int i=0;i<list.size();i++)
			{
				//System.out.println(list.get(i));	
				temp = list.get(i).split("=");
				if(temp[0].contains(first) && temp[1].contains(second))
				{
					result = list.get(i);
					//System.out.println("Result:: "+result);
					list.remove(i);
					break;
				}
				if(temp[1].contains(first) && temp[0].contains(second))
				{
					result = list.get(i);
					//System.out.println("Result:: "+result);
					list.remove(i);
					break;
				}
			}
			bool = true;
		}
		else
		{
			for(int i=0;i<list.size();i++)
			{
				
				temp = list.get(i).split("=");
				if(temp[0].contains(second) || temp[1].contains(second))
				{
					result = list.get(i);
					//System.out.println("Result else:: "+result);
					list.remove(i);
					break;
				}
			}
		}
		
		return result;
	}
	
	
	

	/*
	 * (non-Javadoc)
	 * @see edu.buffalo.cse562.AbstractSelectEvaluator#visit(net.sf.jsqlparser.schema.Table)
	 * Visit FromItem of type Table and creates a Scan Operator for that table.
	 */
	public void visit(Table t)
	{
		List<ColumnDefinition> c = null;
		int j=0;
		String and = " AND ";
		boolean isThere = false;
		boolean isIndexThere = false;
		ArrayList<String> indexWhere = new ArrayList<String>();
		
		
		if(t.getAlias() != null)
		{
			Constants.tableAliasMap.put(t.getAlias(),t.getWholeTableName().toLowerCase());
		}
		
		for(Table table : tables.keySet())
		{
			if(table.getWholeTableName().toLowerCase().equalsIgnoreCase(t.getWholeTableName()))
			{
				c = tables.get(table);
			}
		}
		
		for (int i = 0; i < notEquiJoin.size(); i++) 
        {
			//System.out.println(notEquiJoin.get(i));
            String temp[] = notEquiJoin.get(i).split("\\.");

            if(t.getWholeTableName().trim().equalsIgnoreCase(temp[0].replaceAll("\\(","").trim()))
            {
            	isThere = true;
            	indexWhere.add(notEquiJoin.get(i));
            	
            }
            
        }
		
        StringBuilder tempExpr = new StringBuilder();

        for (int i = 0; i < notEquiJoinSelection.size(); i++) 
        {
        	//System.out.println("FSJ "+forSelectionJoin.get(i));
            String temp[] = notEquiJoinSelection.get(i).split("\\.");

            if(t.getWholeTableName().trim().equalsIgnoreCase(temp[0].replaceAll("\\(","").trim()))
            {
            	if(j==0)
            	{
            		tempExpr.append(notEquiJoinSelection.get(i));
            		j++;
            	}
            	else
            	{
            		tempExpr.append(and+notEquiJoinSelection.get(i));
            	}
            }
            
        }
        if(tempExpr.length() == 0)
        	tempExpr = null;
        
        if(tempExpr != null)
        {
        	StringReader strReader=new StringReader(tempExpr.toString());
        	CCJSqlParser parser = new CCJSqlParser(strReader);

        	try 
        	{
        		onExpr = parser.Expression();
        	} 
        	catch (net.sf.jsqlparser.parser.ParseException e) 
        	{
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	}
        }
		
		/*Expression onExpr = null;
        StringBuilder tempExpr = new StringBuilder();

        for (int i = 0; i < notEquiJoin.size(); i++) 
        {
            String temp[] = notEquiJoin.get(i).split("\\.");

            if(t.getWholeTableName().trim().equalsIgnoreCase(temp[0].replaceAll("\\(","").trim()))
            {
            	isThere = true;
            	if(j==0)
            	{
            		tempExpr.append(notEquiJoin.get(i));
            		j++;
            	}
            	else
            	{
            		tempExpr.append(and+notEquiJoin.get(i));
            	}
            }
            
        }*/

        if(root!=null)
		{
			//System.out.println("not null check "+t.getWholeTableName());
			prevrootTable = rootTable;
			prevroot=root;
		}
        
        if(isThere)
        {
        		String indexW[]  = new String[indexWhere.size()];
        		indexW = indexWhere.toArray(indexW);
        		root = new IndexScanOperator(t, c ,indexW,onExpr);
        }
        else
        {
        	//System.out.println("else "+t.getWholeTableName());
        	//if(onExpr != null)
        	//	root = new ScanOperator(t, c ,dataDir,onExpr,pushingProjection);
        	//else
        		root = new ScanOperator(t, c ,dataDir,pushingProjection);
        }
        
        /*if(isThere)
        {
        	StringReader strReader=new StringReader(tempExpr.toString());
        	CCJSqlParser parser = new CCJSqlParser(strReader);
        	try 
        	{
        		onExpr = parser.Expression();
        		root = new ScanOperator(t, c ,dataDir,onExpr,pushingProjection);
        	} 
        	catch (net.sf.jsqlparser.parser.ParseException e) 
        	{
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	}
        }
        else
        {
        	//System.out.println("else "+t.getWholeTableName());
        	root = new ScanOperator(t, c ,dataDir,pushingProjection);
        }*/
        
		
		
		aggregateTable = t;
		
		if(rootTable == null)
		{
			rootTable = t.getWholeTableName();
		}
		else
			prevrootTable = t.getWholeTableName();
	}
	
	/*
	 * (non-Javadoc)
	 * @see edu.buffalo.cse562.AbstractSelectEvaluator#visit(net.sf.jsqlparser.statement.select.SelectExpressionItem)
	 * Visit SelectItem and 
	 */
	
	
	public void visit(SelectExpressionItem sei) {
		//System.out.println("What expression am I?" + sei.getExpression());
		Constants.aliasMap.put(sei.getAlias(),sei.getExpression().toString());
		sei.getExpression().accept(this);
	}
	
	/*
	 * (non-Javadoc)
	 * @see edu.buffalo.cse562.AbstractSelectEvaluator#visit(net.sf.jsqlparser.statement.select.AllColumns)
	 * Visit of SelectItem; use for * in SELECT * FROM ...
	 */
	public void visit(AllColumns a)
	{
		
	}
	
	public boolean check(String temp1)
	{
		if(temp1.contains("=") && !temp1.contains(" or ") && !temp1.contains(" OR ") && !temp1.contains("<") && !temp1.contains(">"))
			return true;
		return false;
	}
	
	
	
	public void visit(Function f)
	{
		//System.out.println(f.toString());
		if(function==null)
		{
			function = new ArrayList<Function>();
		}
		function.add(f);
	}
	
	public void visit(Column c)
	{
		if(cols == null)
		{
			cols = new ArrayList<Column>();
		}
		//System.out.println(c.toString());
		cols.add(c);
	}
	
	public void visit(Parenthesis prnths)
	{
		prnths.getExpression().accept(this);
	}
	
	public void visit(Subtraction subtraction)
	{
	}
	
	public void visit(Multiplication multiplication)
	{
		
	}
	
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
	//	System.out.println(arg0);
		Constants.subSelect = true;
		arg0.getSelectBody().accept(this);		
		
	}
	
	public Tuple getTuple() 
	{ // TODO Auto-generated method stub 
		return root.readOnetuple(); 
	}


}
