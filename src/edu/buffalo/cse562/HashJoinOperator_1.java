package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class HashJoinOperator_1 implements Operator {

	private Operator left, right;
	Tuple leftTuple, rightTuple;
	private String condition = null;
	private String leftCondition = null;
	private String rightCondition = null;
	private String rightTable = null;
	private String relation = Main.swapDir+"";
	private BufferedReader brRight = null;
	private FileReader frRight = null;
	private int level;
	private Map<String,ArrayList<Tuple>> mapper;
	private int index = 0;
	private File rightfile;
	private boolean isFound = false;
	private ArrayList<Tuple> rtList = null;
	private Iterator<Tuple> rtListItr = null;
	private Map<File,BufferedWriter> fileMap;
	private int relationLeftIndex = 0;
	private int relationRightIndex = 0;
	private boolean isLeftSchemaLoad = false;
	private boolean isRightSchemaLoad = false;
	private ArrayList<String> leftSchemaList;
	private ArrayList<String> rightSchemaList;
	private ArrayList<String> leftSchemaType;
	private ArrayList<String> rightSchemaType;
	private Set<String> leftPartitionFiles;
	private Set<String> rightPartitionFiles;
	

	public HashJoinOperator_1(Operator left, Operator right,String condition,String rightTable,int level)
	{
		this.left = left;
		this.right = right;
		this.condition = condition;
		this.rightTable = rightTable;
		this.level = level;
		this.mapper = new HashMap<String,ArrayList<Tuple>>();
		this.leftSchemaList = new ArrayList<String>();
		this.rightSchemaList = new ArrayList<String>();
		this.leftSchemaType = new ArrayList<String>();
		this.rightSchemaType = new ArrayList<String>();
		this.rtList = new ArrayList<Tuple>();
		this.leftPartitionFiles = new HashSet<String>();
		this.rightPartitionFiles = new HashSet<String>();
		leftInit();
		rightInit();
		//printPartitions();
		load();
	}

	public void leftInit()
	{
		//System.out.println("left Init");
		split();
		Tuple leftTuple = left.readOnetuple();
		int value = 0;
		Tuple tp;
		File file = null;
		int key;
		fileMap = new HashMap<File,BufferedWriter>();
		int j=0;
		int count  =0 ;
		
		if(leftTuple != null)
		{
			try {
				
				if(relationLeftIndex==0)
		        {
					try {
						File relationFile = new File(relation,"left_"+level+"_schema.DAT");
						relationFile.deleteOnExit();
						FileWriter fw=null;
						

						fw = new FileWriter(relationFile.getAbsoluteFile());

						BufferedWriter bw = new BufferedWriter(fw);
						for(Entry<String,Datum> a : leftTuple.entrySet())
						{
							bw.write(a.getValue().getType()+" "+a.getKey()+"|");
						}
						bw.flush();
						fw.close();
						bw.close();
						relationLeftIndex++;
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					
		        }
				
				FileWriter fw=null;
				BufferedWriter out = null ;
				do
				{
					
					tp = leftTuple;
					/*System.out.println(leftCondition);
					for(Entry<String,Datum> a : tp.entrySet())
						System.out.print(a.getKey()+" "+a.getValue().getValue()+"|");
					System.out.println();*/
					//value = leftTuple.get(leftCondition.trim()).getValue().toString();
					value = leftTuple.get(leftCondition.trim()).hashCode();
					key = partition(value);
					file = new File(relation,"left_"+level+"_"+key+ ".ser");
					if(!leftPartitionFiles.contains(file.getAbsolutePath()))
					{
						file.createNewFile();
						leftPartitionFiles.add(file.getAbsolutePath());
						file.deleteOnExit();
						fw = new FileWriter(file.getAbsoluteFile(),true);
						out = new BufferedWriter(fw);
						fileMap.put(file, out);
					}
					else
					{
						out = fileMap.get(file);
					}
						count++;
						out.write(flattenArray(tp,"|")+"\n");
						if(count == 10000)
						{
							out.flush();
							out.close();
							fw = new FileWriter(file.getAbsoluteFile(),true);
							out = new BufferedWriter(fw);
							fileMap.put(file, out);
							count = 0;
						}
						leftTuple = left.readOnetuple();
				}while(leftTuple != null);
				
				//fw.close();
				//out.close();
				
				
				ArrayList<BufferedWriter> objStream_litr = new ArrayList<BufferedWriter>(fileMap.values());
				for(int i=0;i<objStream_litr.size();i++)
				{
					try {
						objStream_litr.get(i).close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				fileMap = null;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("left init end");
	}
	
	public void rightInit()
	{
		//System.out.println("right Init");
		split();
		Tuple rightTuple = right.readOnetuple();
		int value = 0;
		Tuple tp;
		File file = null;
		int count = 0;
		int key;
		fileMap = new HashMap<File,BufferedWriter>();
		
		if(rightTuple != null)
		{
			if(relationRightIndex==0)
	        {
				try {
					File relationFile = new File(Main.swapDir,"right_"+level+"_schema.DAT");
					relationFile.deleteOnExit();
					FileWriter fw=null;

					fw = new FileWriter(relationFile.getAbsoluteFile());

					BufferedWriter bw = new BufferedWriter(fw);
					for(Entry<String,Datum> a : rightTuple.entrySet())
					{
						bw.write(a.getValue().getType()+" "+a.getKey()+"|");
					}
					
					bw.flush();
					fw.close();
					bw.close();
					

				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				relationRightIndex++;
	        }
			try {

				BufferedWriter out = null ;
				FileWriter fw=null;
				do
				{
					
					//value = rightTuple.get(rightCondition.trim()).getValue().toString();
					value = rightTuple.get(rightCondition.trim()).hashCode();
					key = partition(value);
					tp = rightTuple;
					file = new File(relation,"right_"+level+"_"+key+ ".ser");
					if(!rightPartitionFiles.contains(file.getAbsolutePath()))
					{
						file.createNewFile();
						rightPartitionFiles.add(file.getAbsolutePath());
						file.deleteOnExit();
						fw=new FileWriter(file.getAbsoluteFile());
						out = new BufferedWriter(fw);
						fileMap.put(file, out);
					}
					else
					{
						out = fileMap.get(file);
					}
					count++;
					out.write(flattenArray(tp,"|")+"\n");
					if(count == 10000)
					{
						out.flush();
						out.close();
						fw = new FileWriter(file.getAbsoluteFile(),true);
						out = new BufferedWriter(fw);
						fileMap.put(file, out);
						count = 0;
					}
						rightTuple = right.readOnetuple();

				}while(rightTuple != null);
			
				//fw.close();
				//out.close();
				
				
				ArrayList<BufferedWriter> objStream_litr = new ArrayList<BufferedWriter>(fileMap.values());
				for(int i=0;i<objStream_litr.size();i++)
				{
					try {
						objStream_litr.get(i).close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				fileMap = null;
				//rightItString = rightFiles.iterator();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("right init end");
	}
	
	public String flattenArray(Map<String, Datum>  arr, String delimiter)
	{
		String result = "";
		String delim = "";
		for(Entry<String, Datum> entry : arr.entrySet())
		{
			result += delim+entry.getValue().getValue();
			delim = delimiter;
		}
		return result.trim();
	}
	
/*	public void printPartitions()
	{
		File file;
		int j=0;
		try 
		{
				System.out.println("***** LEFT PARTITIONS ********");
				for(int i=0;i<32;i++)
				{
					System.out.println("Partition:: "+i);
					file = new File(Main.swapDir+"\\left_"+level+"_"+i+".ser");
					
					if(file.exists())
					{
						FileInputStream fin = new FileInputStream(file.getAbsolutePath());
						ObjectInputStream in = new ObjectInputStream(fin);
						while(fin.available() > 0)
						{
							Tuple tp = (Tuple)in.readObject();	
							print(tp);
							j++;
						}
					}
				}
				System.out.println("\n No of tuples in left ::"+j);
				System.out.println("*************");
				
				j=0;
				
				System.out.println("***** RIGHT PARTITIONS ********");
				for(int i=0;i<32;i++)
				{
					System.out.println("Partition:: "+i);
					file = new File(Main.swapDir+"\\right_"+level+"_"+i+".ser");
					
					if(file.exists())
					{
						FileInputStream fin = new FileInputStream(file.getAbsolutePath());
						ObjectInputStream in = new ObjectInputStream(fin);
						
						while(fin.available() > 0)
						{
							Tuple tp = (Tuple)in.readObject();	
							print(tp);
							j++;
						}
					}
				}
				System.out.println("\n No of tuples in left ::"+j);
				System.out.println("*************");
		}
		catch(ClassNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/
	
	public int partition(int key)
	{
		return (Math.abs(key) % 8);
	}
	
	public void split()
	{
		String temp[] = condition.split("=");
		//System.out.println("Inside split:: "+temp[0]+" "+temp[1]+" "+rightTable);
		String temp1[] = temp[0].split("\\.");
		if(temp1[0].trim().contains(rightTable.trim()))
		{
			rightCondition = temp[0];
			leftCondition = temp[1];
			//System.out.println("temp[0]");
			
		}
		else
		{
			rightCondition = temp[1];
			leftCondition = temp[0];
			
		}	
			
	}
	
   public void load() 
   {
	   do
	   {
		   if(index < 256)
		   {
			  // System.out.println("Partition no:: "+index);
			   File leftfile = new File(relation,"left_"+level+"_"+index+".ser");
			   File leftschema = new File(relation,"left_"+level+"_schema.DAT");
			   File rightschema = new File(relation,"right_"+level+"_schema.DAT");
			   rightfile = new File(relation,"right_"+level+"_"+index+".ser");
			  /* File leftfile = new File(relation,"right_"+level+"_"+index+".ser");
			   File leftschema = new File(relation,"right_"+level+"_schema.DAT");
			   File rightschema = new File(relation,"left_"+level+"_schema.DAT");
			   rightfile = new File(relation,"left_"+level+"_"+index+".ser");*/
			   this.rtList = new ArrayList<Tuple>();
			   BufferedReader brLeft = null;
			   Tuple tp = null;
			   mapper = new HashMap<String,ArrayList<Tuple>>();
			   

			   try
			   {
				  
				   if(leftschema.exists() && !isLeftSchemaLoad)
				   {
					   BufferedReader br=null;
					   FileReader fr=new FileReader(leftschema.getAbsolutePath());
					   br = new BufferedReader(fr);
					   String line = br.readLine();
					   String temp[] = line.split("\\|");
					   String y = null;
					   for(int i=0;i<temp.length;i++)
					   {
						   y = temp[i];
						   String temp1[] = y.split(" ");
						   leftSchemaList.add(temp1[1]);
						   leftSchemaType.add(temp1[0]);
					   }
					   fr.close();
					   br.close();
					   isLeftSchemaLoad = true;
				   }
				   if(rightschema.exists() && !isRightSchemaLoad)
				   {
					   BufferedReader br=null;
					   FileReader fr=new FileReader(rightschema.getAbsolutePath());
					   br = new BufferedReader(fr);
					   String line = br.readLine();
					   String temp[] = line.split("\\|");
					   String y = null;
					   for(int i=0;i<temp.length;i++)
					   {
						   y = temp[i];
						   String temp1[] = y.split(" ");
						   rightSchemaList.add(temp1[1]);
						   rightSchemaType.add(temp1[0]);
					   }
					   fr.close();
					   br.close();
					   isRightSchemaLoad = true;
				   }
				   if(leftfile.exists() && rightfile.exists())
				   {
					   //  System.out.println("load()::1");
					   FileReader frleft=new FileReader(leftfile.getAbsolutePath());
					   brLeft = new BufferedReader(frleft);
					   //fileInRight = new FileInputStream(rightfile.getAbsolutePath());
					  // inRight = new ObjectInputStream(fileInRight);
					   frRight=new FileReader(rightfile.getAbsolutePath());
					   if(brRight!=null)
					   brRight.close();
					   brRight = new BufferedReader(frRight);
					   String line = null;
					   while(( line = brLeft.readLine()) != null)
					   {
						   //   System.out.println("load()::2");
						   tp = getMap(line,leftSchemaList,leftSchemaType);
						  String key = tp.get(leftCondition.trim()).getValue().toString();
						  // String key = tp.get(rightCondition.trim()).getValue().toString();
						   if(mapper.get(key) != null)
						   {
							   ArrayList<Tuple> a = mapper.get(key);
							   a.add(tp);
							   mapper.put(key,a);
						   }
						   else
						   {
							   ArrayList<Tuple> a = new ArrayList<Tuple>();
							   a.add(tp);
							   mapper.put(key,a);
						   }
					   }
					   frleft.close();
					   brLeft.close();
					   
					   
				   }
			   }
			   catch (FileNotFoundException e) {
				   // TODO Auto-generated catch block
				   e.printStackTrace();
			   } catch (IOException e) {
				   // TODO Auto-generated catch block
				   e.printStackTrace();
			   }

		   }
		   index++;
	   }while(brRight == null);
	   //System.out.println("load end");
} 	
   
public Tuple getMap(String input,ArrayList<String> list,ArrayList<String> type)
{
	Tuple a = new Tuple();
	int i=0;
	String temp[] = input.split("\\|");

	while(i < list.size())
	{
		if(type.get(i).equals("int"))
		{
			//System.out.println(list.get(i)+" "+type.get(i)+" "+temp[i]);
			a.put(list.get(i),new GetInteger(temp[i]));
			i++;
    		continue;
		}
		if(type.get(i).equals("string"))
		{
			//System.out.println(list.get(i)+" "+type.get(i)+" "+temp[i]);
			a.put(list.get(i),new GetString(temp[i]));
			i++;
    		continue;
		}
		if(type.get(i).equals("double"))
		{
			//System.out.println(list.get(i)+" "+type.get(i)+" "+temp[i]);
			a.put(list.get(i),new GetDouble(temp[i]));
			i++;
    		continue;
		}
		if(type.get(i).equals("date"))
		{
			//	Date date1 = out.parse(temp[i]);
			//	String date2 = out1.format(date1);
				a.put(list.get(i),new GetDate(temp[i]));
				i++;
	    		continue;
		}
	}
	
	return a;
}

   	   public void print(Tuple value)
	   {
		   for(Entry<String,Datum> entry : value.entrySet())
			   System.out.print(entry.getKey()+"|"+entry.getValue().getValue()+"|");
		   System.out.println();
	   }
   
	
	@Override
	public Tuple readOnetuple() 
	{
		Tuple right;
		Tuple resultTuple = null;
		String line = null;
		
		if(index == 257)
		{
			try {
				if(frRight!=null)
				frRight.close();
				if(brRight!=null)
				brRight.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			mapper = null;
			return null;
		}
		
		try 
		{
			/*while(fileInRight.available() > 0)
			{
				right = (Tuple)inRight.readObject();
				String key = right.get(rightCondition.trim()).getValue().toString();
				if(mapper.containsKey(key))
				{
					   ArrayList<Tuple> a = mapper.get(key);
					   loadResultList(right,a);
				}
			}*/
			while((line = brRight.readLine()) != null)
			{
				right = getMap(line,rightSchemaList,rightSchemaType);
				String key = right.get(rightCondition.trim()).getValue().toString();
				//String key = right.get(leftCondition.trim()).getValue().toString();
				if(mapper.containsKey(key))
				{
					   ArrayList<Tuple> a = mapper.get(key);
					   loadResultList(right,a);
				}
			}
			//brRight.close();
			mapper = null;
			if(!rtList.isEmpty())
			{
				rtListItr = rtList.iterator();
				
				if(rtListItr.hasNext())
				{
					resultTuple = rtListItr.next();
					//print(resultTuple);
					rtListItr.remove();
				}
			}
			else
			{
				//rtList = null;
				//rtListItr = null;
				load();
				return readOnetuple();
			}
			
			
		
		} 
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return resultTuple;
	}
	
	public void loadResultList(Tuple a,ArrayList<Tuple> b)
	{
		
		for(int i=0;i<b.size();i++)
		{
			rtList.add(merge(a,b.get(i)));
		}
		//System.out.println("result size"+rtList.size());
		
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		left.reset();
		right.reset();
	}

	public Tuple merge(Tuple leftTuple,Tuple rightTuple)
	{

		
		Tuple result = new Tuple();
		for(Entry<String, Datum> entry : leftTuple.entrySet())
		{
			//System.out.print(entry.getKey()+"|"+entry.getValue().getValue());
			result.put(entry.getKey(), entry.getValue());
		}
		//System.out.println();
		for(Entry<String, Datum> entry : rightTuple.entrySet())
		{
			//System.out.print(entry.getKey()+"|"+entry.getValue().getValue());
			result.put(entry.getKey(), entry.getValue());
		}
		//System.out.println();
		return result;
	}

}
