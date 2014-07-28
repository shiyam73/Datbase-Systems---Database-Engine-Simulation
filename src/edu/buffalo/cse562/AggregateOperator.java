package edu.buffalo.cse562;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Join;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

public class AggregateOperator extends AggregateEvaluator implements Operator{

	private ArrayList<Function> function;
	private ArrayList<Column> cols;
	private List groupby;
	private Operator operator;
	String col;
	String []groupbycol;
	private Table table = null;
	String[] colvalues=null;
	String[] typeValues = null;
	int size=0;
	int distinct;
	List selectitems;	
	String type=null;
	private Limit limit;
	static int sum1;
	private String relation = Main.swapDir+"";

	 static HashMap<String,Tuple> hresult;
	 static HashMap<String,Integer> distinctmap;
	 static HashMap<String,Double> distinct_result;
	
	 private List<OrderByElement> orderby = null;
	 PlainSelect ps1;
	 
	public AggregateOperator(Operator operator, PlainSelect ps,ArrayList<Function> functionin, ArrayList<Column> colsin, List groupbyin,Table table,List selitems)
	{
		//System.out.println("inside aggregate operator "+ps.toString());
		selectitems=selitems;
		function=functionin;
		cols=colsin;
		if(groupbyin != null)
		{
			groupby=groupbyin;	
			groupbycol=new String[groupby.size()];
		}
		ps1 = ps;
		this.operator=operator;
		hresult = new HashMap<String,Tuple>();
		distinctmap = new HashMap<String,Integer>();
		distinct_result = new HashMap<String,Double>();
		this.table = table;
		sum1=0;
	}

public void groupByCount(Tuple input, String function_name,int flag)
{
	String grpcol=null;
	//System.out.println("inside sum "+(int)input.get(col).getValue());
	Tuple result = new Tuple();
	result=input;
	for(int i=0;i<groupbycol.length;i++)
	{
		if(i==0)
		{
			grpcol=input.get(groupbycol[i]).getValue()+"";
			type = input.get(groupbycol[i]).getType()+"";
		}
		else
			{
			grpcol=grpcol +"|"+ (input.get((groupbycol[i])).getValue());
			type += "|"+input.get(groupbycol[i]).getType();
			
			}

	}
	if(flag==0)
	{
		if(!hresult.containsKey(grpcol))
		{
			result.put(function_name, new GetDouble("1.0"));
			hresult.put(grpcol, result);
		}
		else
		{
			result=hresult.get(grpcol);
			if(result.containsKey(function_name))
			{
				Double d=(Double)result.get(function_name).getValue();
				Double count = d+1.0;
				result.put(function_name,new GetDouble(count.toString()));
				hresult.put(grpcol, result);
			}
			else
			{
				result.put(function_name, new GetDouble("1.0"));
				hresult.put(grpcol, result);
			}
		}
	}
	else
	{
		
		String disgrp = grpcol+"|"+input.get(col.replaceAll("\\(|\\)","")).getValue();
		//System.out.println("COL "+col+" "+disgrp);
		if(!distinctmap.containsKey(disgrp))
		{
			sum1++;
			if(distinct_result.containsKey(grpcol))
			{
				Double count = (Double)distinct_result.get(grpcol);
				count = count+1.0;
				distinct_result.put(grpcol, count);
				result=hresult.get(grpcol);
				result.put(function_name, new GetDouble(count.toString()));
				hresult.put(grpcol, result);

			}
			else
			{
				Double count = 1.0;
				distinct_result.put(grpcol, count);
				result.put(function_name, new GetDouble(count.toString()));
				hresult.put(grpcol, result);
			}
			
			distinctmap.put(disgrp, 1);
		}
	}
}

public void groupBySum(Tuple tuple,String function)
{
	
	//System.out.println(function);
	String grpcol=null;
	Tuple result = new Tuple();
	if(groupby != null)
	{
		//System.out.println(groupbycol.length);
		for(int i=0;i<groupbycol.length;i++)
		{
			if(i==0)
			{
				grpcol=input.get(groupbycol[i]).getValue().toString()+"";
				type = input.get(groupbycol[i]).getType()+"";
			}
			else
			{
				grpcol=grpcol +"|"+(input.get((groupbycol[i])).getValue().toString());
				type += "|"+input.get(groupbycol[i]).getType();
			}

		}
		if(hresult.containsKey(grpcol))
		{
			
			Tuple sum_result = hresult.get(grpcol);
			if(sum_result.containsKey(function))
			{
				Double sum=(Double)sum_result.get(function).getValue();
				sum= sum+(Double)input.get(col.replaceAll("\\(|\\)","")).getValue();
				sum_result.put(function, new GetDouble(sum.toString()));
				hresult.put(grpcol, sum_result);
			}
			else
			{
				Double sum=(Double)input.get(col.replaceAll("\\(|\\)","")).getValue();
				sum_result.put(function, new GetDouble(sum.toString()));				
			}
			
		}
		else
		{
			Tuple sum_result = new Tuple();
			sum_result=input;
			Double sum=(Double)input.get(col.replaceAll("\\(|\\)","")).getValue();
			sum_result.put(function, new GetDouble(sum.toString()));			
		    hresult.put(grpcol,sum_result);
		}
	}
	else
	{
		if(hresult.containsKey(grpcol))
		{
			Tuple sum_result = hresult.get(function);
			Double sum=(Double)sum_result.get(function).getValue();
			sum= sum+(Double)input.get(function.replaceAll("\\(|\\)","")).getValue();
			sum_result.put(function, new GetDouble(sum.toString()));
			hresult.put(grpcol, sum_result);
		}
		else
		{
			Tuple sum_result = input;
			Double sum=(Double)input.get(function.replaceAll("\\(|\\)","")).getValue();
			sum_result.put(function, new GetDouble(sum.toString()));			
		    hresult.put(grpcol,sum_result);
			
		}
	}
	
		
}
public void groupByAvg(Tuple input,String function)
{	
	groupBySum(input,function+"savg");
	groupByCount(input, function+"cavg",0);
	
}
public void groupByMax(Tuple input, String function)
{
	String grpcol=null;
	for(int i=0;i<groupbycol.length;i++)
	{
		if(i==0)
		{
			grpcol=input.get(groupbycol[i]).getValue()+"";
			type = input.get(groupbycol[i]).getType()+"";
		}
		else
			{
			grpcol=grpcol +"|"+ (input.get((groupbycol[i])).getValue())+"";
			type += "|"+input.get(groupbycol[i]).getType();
			
			}

	}
	
	if(hresult.containsKey(grpcol))
	{
		Tuple max_result = hresult.get(grpcol);
		int max=(int)max_result.get(function).getValue();
		if((int)input.get(col).getValue()>max)
		max_result.put(function,new GetInteger(input.get(col).getValue().toString()));
		hresult.put(grpcol, max_result);
	}
	else
	{
		Tuple max_result=input;
		max_result.put(function, new GetInteger(input.get(col).getValue().toString()));
		hresult.put(grpcol, max_result);
	}
	
}

public void groupByMin(Tuple input, String function)
{
	String grpcol=null;
	for(int i=0;i<groupbycol.length;i++)
	{
		if(i==0)
		{
			grpcol=input.get(groupbycol[i]).getValue()+"";
			type = input.get(groupbycol[i]).getType()+"";
		}
		else
			{
			grpcol=grpcol + "|"+(input.get((groupbycol[i])).getValue())+"";
			type += "|"+input.get(groupbycol[i]).getType();
			
			}

	}
	
	if(hresult.containsKey(grpcol))
	{
		Tuple max_result = hresult.get(grpcol);
		int max=(int)max_result.get(function).getValue();
		if((int)input.get(col).getValue()<max)
		max_result.put(function,new GetInteger(input.get(col).getValue().toString()));
		hresult.put(grpcol, max_result);
	}
	else
	{
		Tuple max_result=input;
		max_result.put(function, new GetInteger(input.get(col).getValue().toString()));
		hresult.put(grpcol, max_result);
	}
}

@Override
public Tuple readOnetuple()
{

	// TODO Auto-generated method stub
	ArrayList arrResult = new ArrayList<LinkedHashMap<String,Datum>>();
	
	if(groupby!=null)
	{
		
		for(int j=0;j<groupby.size();j++)
		{
			if(groupby.get(j).toString().contains("."))
				groupbycol[j]=groupby.get(j).toString();
			else
				groupbycol[j]=table.getWholeTableName()+"."+groupby.get(j).toString();
		}
		
		
	
			
			//hsum = new HashMap<String,Double>();
		//	System.out.println("started "+col+" "+func);
		int abc=0;
			do
			{
				input=operator.readOnetuple();
				
				if(input!=null)
				{
				abc++;
				
					for(int i=0;i<function.size();i++)
					{		
						String func=function.get(i).getName().toLowerCase();
						try
						{
							col=function.get(i).getParameters().toString();
						}
						catch(NullPointerException e)
						{
							col=groupbycol[0];
						}
					
						col = col.replaceAll("\\(|\\)","");
						
					String tempFunc = function.get(i).toString();
					if(func.equals("sum"))
					{
						//System.out.println("inside sum: "+function.get(i).getParameters().toString());
						//System.out.println(function.get(i).toString());
						
						if(Constants.aliasMap.get(tempFunc) != null)
							alias = Constants.aliasMap.get(tempFunc);
						function.get(i).getParameters().accept(this);
						groupBySum(input,tempFunc);
						continue;
						
					}
					if(func.equals("count"))
					{

						if(tempFunc.contains("distinct") || tempFunc.contains("DISTINCT"))
						{
							distinct=1;
							groupByCount(input,tempFunc,distinct);
						}
						else
						{
							distinct=0;
							groupByCount(input,tempFunc,distinct);
						}
						continue;
					}
					if(function.get(i).getName().equalsIgnoreCase("AVG"))
					{
						
						groupByAvg(input,function.get(i).toString());
					}
					if(function.get(i).getName().equalsIgnoreCase("MAX"))
					{
						groupByMax(input,function.get(i).toString());
					}
					if(function.get(i).getName().equalsIgnoreCase("MIN"))
					{
						groupByMin(input,function.get(i).toString());
					}
				}
			}
			}while(input!=null);
			
	
			for(int i=0;i<function.size();i++)
			{
				if(function.get(i).getName().equalsIgnoreCase("AVG"))
				{
					for (Entry<String, Tuple> entry : hresult.entrySet()) 
					{
						Tuple result = entry.getValue();
						
							Double sum=(Double)result.get(function.get(i).toString()+"savg").getValue();
							//System.out.println("SUM IS : "+entry1.getValue());
							Double count1=(Double)result.get(function.get(i).toString()+"cavg").getValue();
							//System.out.println("COUNT IS :"+entry.getValue());
							Double avg = (sum/count1);
							//System.out.println(function.get(i).toString());
							result.put(function.get(i).toString(), new GetDouble(avg.toString()));
						

					}
				}
			}
		
		
		
		String aggvalue=null;
		int k=0;
		
		for (Entry<String, Tuple> entry : hresult.entrySet())
		{
			Tuple result=entry.getValue();
		
		
				for(int a=0;a<groupbycol.length;a++)
				{   
					colvalues = entry.getKey().toString().split("\\|");
					//System.out.println(colvalues[0]);
					typeValues = type.split("\\|");
					if(typeValues[a].equals("int"))
						result.put(groupbycol[a], new GetInteger(colvalues[a]));
					if(typeValues[a].equals("string"))
						result.put(groupbycol[a], new GetString(colvalues[a]));
					if(typeValues[a].equals("double"))
						result.put(groupbycol[a], new GetDouble(colvalues[a]));
					if(typeValues[a].equals("date"))
					{
						DateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						DateFormat inputDateFormat = new SimpleDateFormat("E MMM d HH:mm:ss zzz yyyy");
						inputDateFormat.setLenient(false);
						outputDateFormat.setLenient(false);
						Date date = null;
						try {
							date = inputDateFormat.parse(colvalues[a]);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						//System.out.println(colvalues[a]);
						 
						result.put(groupbycol[a], new GetDate(outputDateFormat.format(date).toString()));
					}
				}
				/*for(Entry<String,Datum> a : result.entrySet())
					System.out.print(a.getKey()+"|");
				System.out.println();*/
				arrResult.add(result);
			
			
		}
		
		/*-----------------------ADITYAS ORDER BY*******************/
		orderby=ps1.getOrderByElements();

		//printArrayDiff(arrResult);
		//System.out.println(arrResult.size());
		int b=0;
		if(orderby!=null)
		{
			Collections.reverse(orderby);
			for(OrderByElement obe : orderby)
			{
				String tempa;
				String order = "asc";
				if(obe.isAsc())
				{
					if(Constants.aliasMap.get(obe.getExpression().toString()) != null)
					{
						tempa = Constants.aliasMap.get(obe.getExpression().toString());
						//System.out.println("Orderby "+tempa);
						if(tempa.contains("."))
						{
							//System.out.println("inside alias checking");
							String temp[] = tempa.split("\\.");
							if(Constants.tableAliasMap.containsKey(temp[0]))
							{
								tempa = Constants.tableAliasMap.get(temp[0])+"."+obe.getExpression().toString();
								
							}
						}
					}
					else
					{
						if(obe.getExpression().toString().contains("."))
							tempa=obe.getExpression().toString();
						else
							tempa = table.getWholeTableName().toString()+"."+obe.getExpression().toString();
					}
					//System.out.println(tempa);
					Collections.sort(arrResult, new GetComparator(tempa,"ASC"));
					//System.out.println("Relation "+tempa);
					//trySort1 ty = new trySort1(arrResult,tempa,order);
					//arrResult = ty.externalSort();
					
					/*for(int i=0;i < arrResult.size();i++ )
					{
						Tuple x = (Tuple)arrResult.get(i);
						for(Entry<String,Datum> a : x.entrySet())
							System.out.print(a.getValue().getValue()+" ");
						System.out.println();
					}
					System.out.println("*******************END*******************");*/
					
				}
				else
				{
					String tempd=null;
					order = "desc";
					//System.out.println("hello \n");
					if(Constants.aliasMap.get(obe.getExpression().toString()) != null)
						tempd = Constants.aliasMap.get(obe.getExpression().toString());
					else
					{
					if(obe.getExpression().toString().contains("."))
						tempd=obe.getExpression().toString();
					else
						tempd = table.getWholeTableName().toString()+"."+obe.getExpression().toString();
					}
					//System.out.println(tempd);
				//	trySort1 ty = new trySort1(arrResult,tempd,order);
				//	arrResult = ty.externalSort();
					Collections.sort(arrResult, new GetComparator(tempd,"DESC"));

				}
			}
			//reArrange(arrResult);
			//System.out.println("abcd");
			/*for(int i=0;i < arrResult.size();i++ )
			{
				Tuple x = (Tuple)arrResult.get(i);
				for(Entry<String,Datum> a : x.entrySet())
					System.out.print(a.getValue().getValue()+" ");
				System.out.println();
			}
			System.out.println("*******************END*******************");*/
			b=1;
		}
		/*---------------------------------------------------------*/
		if(b != 1)
		printArray(arrResult);
		//System.out.println("this is limit:"+limit.getRowCount());
		printArray(arrResult);

		
	}
	else
	{

		do
		{
			input=operator.readOnetuple();
			for(int i=0;i<function.size();i++)
			{	

				String func=function.get(i).getName();
				try
				{
					col=function.get(i).getParameters().toString();
				}
				catch(NullPointerException e)
				{
					col=groupbycol[i];

				}
				col = col.replaceAll("\\(|\\)","");
				if(!col.contains("."))
					col = table.getWholeTableName()+"."+col;

				//System.out.println();
				if(input!=null)
				{//System.out.println("here");
				/*	for(Entry<String,Datum> entry : input.entrySet())
					{
						System.out.print(entry.getKey()+ " "+entry.getValue().getValue()+"|");
					}*/
					//System.out.println();
					if(function.get(i).getName().equalsIgnoreCase("SUM"))
					{
						//System.out.println("inside sum");

						function.get(i).getParameters().accept(this);
						if(function.get(i).toString().contains(" AS "))
						{ 
							String temp[] = function.get(i).toString().split(" AS "); 
							alias = temp[1]; 
						}
						groupBySum(input,function.get(i).getParameters().toString());
					}
					if(function.get(i).getName().equalsIgnoreCase("COUNT"))
					{
						groupByCount(input,function.get(i).toString(),0);
					}
					if(function.get(i).getName().equalsIgnoreCase("AVG"))
					{
						groupByAvg(input,function.get(i).getParameters().toString());
					}
					if(function.get(i).getName().equalsIgnoreCase("MAX"))
					{
						groupByMax(input, function.get(i).toString());
					}
					if(function.get(i).getName().equalsIgnoreCase("MIN"))
					{
						groupByMin(input, function.get(i).toString());
					}
				}
			}
		}while(input!=null);
		
		for (Entry<String, Tuple> entry : hresult.entrySet())
		{
			Tuple result=entry.getValue();
			arrResult.add(result);
		}
		
	
		
		printArray(arrResult);

	}
	return null;
}

@Override
public void reset() {
	// TODO Auto-generated method stub
	
}

public void printArray(List arrayList)
{
	/*for(int i=0;i<arrayList.size();i++)
	{
		Tuple result = (Tuple)arrayList.get(i);
	for(Entry<String,Datum> a : result.entrySet())
		System.out.print(a.getKey()+"|"+a.getValue().getValue()+" ");
	System.out.println();
	}*/
	int start;
	int end;
	
	if(ps1.getLimit()!=null)
	{
		limit=ps1.getLimit();
		 end =(int)limit.getRowCount();
		 start = (int)limit.getOffset();
		 if(arrayList.size()<limit.getRowCount())
			{
				end=arrayList.size();
			}
		
	}
	else
	{
		start = 0;
		 end = arrayList.size();
	}
	
	
	BufferedWriter out=null;
	try {
		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(java.io.FileDescriptor.out), "ASCII"), 512);
	} catch (UnsupportedEncodingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}	
	for(int i=start;i<end;i++)
	{
		String separator="";
		Tuple result =  (Tuple) arrayList.get(i);
		for(int j=0;j<selectitems.size();j++)
		{ 
			//System.out.println(selectitems.get(j));
			if(selectitems.get(j).toString().contains("COUNT(") || selectitems.get(j).toString().contains("count("))
			{
				if(selectitems.get(j).toString().contains(" AS "))
				{
					//System.out.println(selectitems.get(j));
					String temp[] = selectitems.get(j).toString().split("AS");
					//double d = (double)Double.parseDouble((String) result.get(temp[0].trim()).getValue());
					double d = (double)result.get(temp[0].trim()).getValue();
					int a = (int)d;
					try {
						out.write(separator+a);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
				else
				{
					/*System.out.println("PPP "+selectitems.get(j).toString());
					for(Entry<String,Datum> a : result.entrySet())
						System.out.print(a.getKey()+"|"+a.getValue().getValue()+" ");
					System.out.println();*/
					//Double d = (Double)result.get(selectitems.get(i).toString()).getValue();
					//int a = (int)d;
					//double d = (double)Double.parseDouble((String)result.get(selectitems.get(j).toString()).getValue());
					double d = (double)result.get(selectitems.get(j).toString()).getValue();
					int a = (int)d;
					try {
						out.write(separator+a);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			else
			{
				if(selectitems.get(j).toString().contains(" AS ") || selectitems.get(j).toString().contains(" as "))
				{
					String temp[] = selectitems.get(j).toString().split("AS");
					if(result.get(temp[0].trim())!=null)
						try {
							out.write(separator+result.get(temp[0].trim()).getValue());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
				else
				{
					if(selectitems.get(j).toString().contains("."))
					{
						if(result.get(selectitems.get(j).toString())!=null)
						{
							if(result.get(selectitems.get(j).toString()).getValue().toString().equalsIgnoreCase("date"))
							{
								SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");


								String date = simpleDateFormat.format((Date)result.get(selectitems.get(j).toString()).getValue());

								try {
									out.write(separator+date);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							} 
							else
							{
								/*System.out.println(selectitems.get(j));
									for(Entry<String,Datum> entry : result.entrySet())
										System.out.print(entry.getKey()+"|"+entry.getValue().getValue()+" ");
									System.out.println();*/
								try {
									out.write(separator+result.get(selectitems.get(j).toString()).getValue());
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					}
					else if(result.get(selectitems.get(j).toString())!=null)
					{

						try {
							out.write(separator+result.get(selectitems.get(j)).getValue());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else	
					{
						//System.out.println(table.getWholeTableName()+"."+selectitems.get(j).toString()+" check ");
						//System.out.println(table.getWholeTableName()+"."+selectitems.get(j).toString());


						if(result.get(table.getWholeTableName()+"."+selectitems.get(j).toString())!=null)
							try {
								out.write(separator+result.get(table.getWholeTableName()+"."+selectitems.get(j)).getValue());
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					}
				}
			}
			separator = "|";
		}
		try {
			out.write("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	try {
		out.flush();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	//System.out.println("***********END*********");

}
public void printArrayDiff(List arrayList)
{
	for(int i=0;i<arrayList.size();i++)
	{
		Tuple t = (Tuple)arrayList.get(i);
		for(Entry<String,Datum> entry : t.entrySet())
			System.out.print(entry.getKey()+" "+entry.getValue().getValue()+" ");
		System.out.println();
	}
}








}
