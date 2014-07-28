package edu.buffalo.cse562;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryHashMap;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class StatementEvaluator extends AbstractStatementVisitor
{
	private String dataDir = null;
	private Map<Table, List<ColumnDefinition>> tables = new HashMap<Table, List<ColumnDefinition>>();
	private ArrayList<String> columnNames;
	private ArrayList<String> typeOfMaps;
	private ArrayList<String> colName;
	private ArrayList<String> colType;
	ScanOperator scan;
	
	public StatementEvaluator(String dataDir)
	{
		this.dataDir = dataDir;
	}

	public void visit(CreateTable ct)
	{
		//System.out.println(ct.getTable().getWholeTableName());
		
		if(Main.build != 0)
		{
			columnNames = new ArrayList<String>();
			typeOfMaps = new ArrayList<String>();
			tables.put(ct.getTable(), ct.getColumnDefinitions());
			scan = new ScanOperator(ct.getTable(), ct.getColumnDefinitions(), dataDir,"");
			colType = scan.getColType();
			colName = scan.getColName();

			if(ct.getTable().getName().equalsIgnoreCase("orders"))
			{
				columnNames.add("orders.orderkey");
				columnNames.add("orders.orderdate");

				typeOfMaps.add("primarytree");
				typeOfMaps.add("secondarytree");

				IndexOperator index = new IndexOperator(scan,columnNames,typeOfMaps,Main.getIndexdir());

			}
			else if(ct.getTable().getName().equalsIgnoreCase("lineitem"))
			{
				columnNames.add("lineitem.orderkey|lineitem.linenumber");
				columnNames.add("lineitem.returnflag");
				columnNames.add("lineitem.shipdate");
				columnNames.add("lineitem.receiptdate");
				columnNames.add("lineitem.shipmode");

				typeOfMaps.add("primarytree");
				typeOfMaps.add("secondaryhashmap");
				typeOfMaps.add("secondarytree");
				typeOfMaps.add("secondarytree");
				typeOfMaps.add("secondaryhashmap");

				IndexOperator index = new IndexOperator(scan,columnNames,typeOfMaps,Main.getIndexdir());

				//read(scan.getTableName());
			}
			else if(ct.getTable().getName().equalsIgnoreCase("part"))
			{
				columnNames.add("part.partkey");
				columnNames.add("part.brand");

				typeOfMaps.add("primarytree");
				typeOfMaps.add("secondaryhashmap");

				IndexOperator index = new IndexOperator(scan,columnNames,typeOfMaps,Main.getIndexdir());
			}
		}
		else
		{
			tables.put(ct.getTable(), ct.getColumnDefinitions());
			Constants.mapIndices.put("orders.orderdate","secondarytree");
			Constants.mapIndices.put("lineitem.returnflag","secondaryhashmap");
			Constants.mapIndices.put("lineitem.shipdate","secondarytree");
			Constants.mapIndices.put("lineitem.receiptdate","secondarytree");
			Constants.mapIndices.put("lineitem.shipmode","secondaryhashmap");
			Constants.mapIndices.put("part.brand","secondaryhashmap");
		}
		//IndexOperator index = new IndexOperator(scan,columnNames,typeOfMaps,Main.getIndexdir());
			
	}
	
	public void visit(Select s)
	{
		if(Main.build == 0)
		{
			SelectEvaluator se = new SelectEvaluator(dataDir, tables);
			s.getSelectBody().accept(se);
			Tuple tpl=null;
			int count =0;
			OutputStream out = new BufferedOutputStream(System.out);
			BufferedWriter out1 = null;
			try 
			{
				out1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(java.io.FileDescriptor.out),"ASCII"),512);
			} 
			catch (UnsupportedEncodingException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			do
			{
				tpl = se.getTuple();
				String separator = "";

				if(tpl!=null)
				{
					for (Map.Entry<String, Datum> entry : tpl.entrySet()) 
					{
						if(entry.getValue().getType().equalsIgnoreCase("date"))
						{
							SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
							String date = simpleDateFormat.format((Date)entry.getValue().getValue());
							try {
								out1.write(separator+date);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//System.out.print(separator+date);
							continue;
						}
						try {
							out1.write(separator+entry.getValue().getValue());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//System.out.print(separator+entry.getValue().getValue());

						separator = "|";	
					}
					try {
						out1.write("\n");
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//System.out.println();
					count++;
				}


			}while(tpl!=null);
			try {
				out1.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void read(String name)
	{
		try
		{
			RecordManager manager = RecordManagerFactory.createRecordManager(Main.indexDir+"\\"+name);
			
			SecondaryTreeMap<String, Tuple, Tuple> secondaryIndex;
			SecondaryHashMap<String, Tuple, Tuple> secondaryHashIndex;
			
			
			String[] temp = getStringArray(columnNames.get(0));
			PrimaryTreeMap<Tuple, Tuple> primaryIndex = manager.treeMap(columnNames.get(0),new RowSerializer(colType,colName),
					new RowSerializer(getKeyType(temp), getKeyname(temp)));

			for(final Entry<String, String> e : Constants.mapIndices.entrySet())
			{
				if(e.getValue().equals("secondaryhashmap"))
				{
					secondaryHashIndex = primaryIndex.secondaryHashMap(e.getKey().trim(), 
							new SecondaryKeyExtractor<String, Tuple, Tuple>() {
								public String extractSecondaryKey(Tuple key,Tuple value) {
									//System.out.println((String)value.get("orders.clerk").getValue().toString());
									return (String)value.get(e.getKey().trim()).getValue().toString();
								}
							});
					System.out.println(e.getKey().trim()+" "+secondaryHashIndex.size());
				}
				else if(e.getValue().equals("secondarytree"))
				{
					secondaryIndex = primaryIndex.secondaryTreeMap(e.getKey().trim(), 
							new SecondaryKeyExtractor<String, Tuple, Tuple>() {
								public String extractSecondaryKey(Tuple key,Tuple value) {
									//System.out.println((String)value.get("orders.clerk").getValue().toString());
									return (String)value.get(e.getKey().trim()).getValue().toString();
								}
							});
					System.out.println(e.getKey().trim()+" "+secondaryIndex.size());
				}
			}

		} 
		catch (IOException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public String[] getStringArray(String key)
	{
		String[] temp = null;
		
		if(key.contains("|"))
		{
			temp = key.split("\\|");
		}
		else
		{
			temp = new String[]{key};
		}
		return temp;
	}
	

	public ArrayList<String> getKeyType(String[] key)
	{
		ArrayList<String> a = new ArrayList<String>();
		
		for(int j=0;j<key.length;j++)
		{
			for(int i=0;i< colName.size();i++)
			{
				if(colName.get(i).equals(key[j]))
				{
					a.add(colType.get(i));
					break;
				}
			}
		}
		return a;
	}
	
	public ArrayList<String> getKeyname(String[] key)
	{
		ArrayList<String> a = new ArrayList<String>();
		
		for(int j=0;j<key.length;j++)
		{
			for(int i=0;i< colName.size();i++)
			{
				if(colName.get(i).equals(key[j]))
				{
					a.add(colName.get(i));
					break;
				}
			}
		}
		return a;
	}
	

}
