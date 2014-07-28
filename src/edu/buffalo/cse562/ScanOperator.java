package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ScanOperator extends ExpressionEvaluator implements Operator {
	
	private Table table;
	private File dataFile;
	private BufferedReader fileReader;
	private String dataDir;
	private List<ColumnDefinition> dataType;
	private Datum data;
	private Expression condition;
	private boolean flag1,flag2;
	private String projection = null;
	private ArrayList<String> colType;
	private ArrayList<String> colName;
	private String tableName = null;

	
	public ScanOperator(Table table, List<ColumnDefinition> colDefs, String dataDir,Expression condition,String projection) 
	{
		this.table = table;
		this.dataDir = dataDir;
		this.dataType = colDefs;
		this.condition = condition;
		this.projection = projection;
		try
		{
			init();
		} 
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ScanOperator(Table table, List<ColumnDefinition> colDefs, String dataDir,String projection) 
	{
		this.table = table;
		this.dataDir = dataDir;
		this.dataType = colDefs;
		this.condition = null;
		this.projection = projection;
		try 
		{
			init();
		} 
		catch (FileNotFoundException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	void init() throws FileNotFoundException
	{
		dataFile = new File(dataDir + "/" + table.getWholeTableName() + ".dat");
		fileReader = new BufferedReader( new FileReader(dataFile));
		colType = new ArrayList<String>();
		colName = new ArrayList<String>();
				
		if(table.getAlias()!=null)
		{
			tableName = table.getAlias().toLowerCase();
		}
		else
		{
			tableName = table.getWholeTableName().toLowerCase();
		}
		
		for(ColumnDefinition colDef : dataType)
		{
			String type = colDef.getColDataType().toString().toLowerCase();
			String key = null;

			key = tableName + "." + colDef.getColumnName();
			
			/*if(table.getAlias()!= null)
			{
				key = table.getAlias() + "." + colDef.getColumnName();
			}
			else
			{
				key = table.getWholeTableName() + "." + colDef.getColumnName();

			}*/
			colType.add(type);
			colName.add(key);
		}
	}

	

	@SuppressWarnings("resource")
	@Override
	public Tuple readOnetuple() {
		// TODO Auto-generated method stub

		Tuple input = new Tuple();

		String readLine = null;
		try 
		{
			do
			{
				readLine = fileReader.readLine();
				if(readLine!=null)
				{
					String s[] = readLine.split("\\|");
					int index = 0;
					//Iterator<ColumnDefinition> itr = dataType.iterator();
					//for(ColumnDefinition colDef : dataType)
					//{
						//ColumnDefinition cd = itr.next();
					while(index < s.length)
					{
						String check = colType.get(index);
						String key = colName.get(index);
						
							//if(projection.contains(key))
							//{
								//System.out.println(key+" | ");
								if(check.equals("int"))
								{
									GetInteger data;
									data = new GetInteger(s[index]);
									input.put(key, data);
								}

								if(check.equals("double") || check.equals("decimal"))
								{
									data = new GetDouble(s[index]);
									input.put(key, data);
								}

								if(check.equals("date"))
								{
									data = new GetDate(s[index]);
									input.put(key, data);
								}

								if(check.equals("string") || check.contains("char"))
								{
									data = new GetString(s[index]);
									input.put(key, data);
								}
							//}
						
							
						index++;
					}
					//}
					/*for(Entry<String,Datum> a : input.entrySet())
						System.out.print(a.getKey()+"|"+a.getValue().getValue()+" ");
					System.out.println();*/
					if(condition!=null)
					{
						//System.out.println(condition);
						flag1=true;
						flag2=getOutput(condition,table,input);
						if(!flag2)
							resetFlag();
					}
					else
					{
						flag1=false;
						flag2=false;
					}
				}
				else
				{
					input=null;
					break;
				}
			}while(flag1 && !flag2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
			return input;

	}

	public ArrayList<String> getColType() {
		return colType;
	}

	public ArrayList<String> getColName() {
		return colName;
	}

	public String getTableName()
	{
		return tableName;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try {
			fileReader = new BufferedReader( new FileReader(dataFile));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
