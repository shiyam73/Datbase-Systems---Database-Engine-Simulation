package edu.buffalo.cse562;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class trySort1 {

	// readers and writers needed for the files
	private static String attribute = "F";
	static ArrayList<Map<String,String>> list;
	static String relation =Main.swapDir+"\\TEMPORARY";
	
	// the main() function
	private static List<Tuple> arrayList = new ArrayList<Tuple>();
	static ArrayList<String> header;
	static ArrayList<String> type;
	private boolean someFileStillHasRows = false;
	private static String sortBy = null;
	
	public trySort1(ArrayList<Tuple> list,String attr,String order)
	{
		this.arrayList = list;
		this.attribute = attr;
		this.header = new ArrayList<String>();
		this.type = new ArrayList<String>();
		this.sortBy = order;
	
	}

	public ArrayList<Tuple> externalSort()
	{
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		try
		{
			Tuple row = new Tuple();
			row.put("dummy", new GetString("dummy"));
			ArrayList<Tuple> tenKRows = new ArrayList<Tuple>();
			
			for(Entry<String,Datum> a : arrayList.get(0).entrySet())
			{
				header.add(a.getKey());
				type.add(a.getValue().getType());
			}
			//System.out.println(header.toString());
			int numFiles = 0;
			int j=0;
			int size = (int)estimateBestSizeOfBlocks(arrayList.size());
			//int size = 10000;
			//System.out.println(j+" "+size);
			while (j < arrayList.size())
			{
				// get 10k rows
				for(int i=0; i<size; i++)
				{
					//String line = initRelationReader.readLine();
					if(j == arrayList.size())
						break;
					else
					{
						row = arrayList.get(j);
							/*for(Entry<String,Datum> entry : row.entrySet())
								System.out.print(entry.getKey()+"|"+entry.getValue().getValue()+" ");
							System.out.println();*/
						tenKRows.add(row);
						j++;
					}
				}
				// sort the rows
				//System.out.println(j);
				tenKRows = mergeSort(tenKRows);

				// write to disk
				File temp = new File(Main.swapDir,"TEMPORARY_chunk" + numFiles + ".DAT");
				FileWriter fw = new FileWriter(temp.getAbsolutePath());
				temp.deleteOnExit();
				BufferedWriter bw = new BufferedWriter(fw);
				//bw.write(flattenArray(header,",")+"\n");
				for(int i=0; i<tenKRows.size(); i++)
				{
					bw.append(flattenArray(tenKRows.get(i),"|")+"\n");
				}
				bw.close();
				numFiles++;
				tenKRows.clear();
			}

			result = mergeFiles(relation, numFiles);


			

		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			System.exit(-1);
		}
		return result;
		


	}
	
	public long estimateBestSizeOfBlocks(int size) {
       
        // we don't want to open up much more than 1024 temporary files, better run
        // out of memory first. (Even 1024 is stretching it.)
        
        long blocksize = size / 1500;
        // on the other hand, we don't want to create many temporary files
        // for naught. If blocksize is smaller than half the free memory, grow it.
        long freemem = Runtime.getRuntime().freeMemory();
        if( blocksize < freemem/2)
            blocksize = freemem/2;
        else {
            if(blocksize >= freemem) 
              System.err.println("We expect to run out of memory. ");
        }
        return blocksize;
    }
 

	// sort an arrayList of arrays based on the ith column
		private static ArrayList<Tuple> mergeSort(ArrayList<Tuple> arr)
		{
			ArrayList<Tuple> left = new ArrayList<Tuple>();
			ArrayList<Tuple> right = new ArrayList<Tuple>();
			if(arr.size()<=1)
				return arr;
			else
			{
				int middle = arr.size()/2;
				for (int i = 0; i<middle; i++)
					left.add(arr.get(i));
				for (int j = middle; j<arr.size(); j++)
					right.add(arr.get(j));
				left = mergeSort(left);
				right = mergeSort(right);
				return merge(left, right);

			}

		}

	
	private ArrayList<Tuple> mergeFiles(String relation, int numFiles)
	{
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		try
		{
			ArrayList<FileReader> mergefr = new ArrayList<FileReader>();
			ArrayList<BufferedReader> mergefbr = new ArrayList<BufferedReader>();
			ArrayList<Tuple> filerows = new ArrayList<Tuple>();
			
			File temp = new File(Main.swapDir,"TEMPORARY_sorted.DAT");
			temp.deleteOnExit();
			FileWriter fw = new FileWriter(temp.getAbsolutePath());
			BufferedWriter bw = new BufferedWriter(fw);

			

			for (int i=0; i<numFiles; i++)
			{
				File tempFile = new File(Main.swapDir,"TEMPORARY_chunk"+i+".DAT");
				mergefr.add(new FileReader(tempFile));
				mergefbr.add(new BufferedReader(mergefr.get(i)));
				// get each one past the header


				// get the first row
				String line = mergefbr.get(i).readLine();
				//System.out.println(line);
				if (line != null)
				{
					filerows.add(getMap(line.split("\\|")));
					someFileStillHasRows = true;
				}
				else 
				{
					filerows.add(null);
				}

			}


			Tuple row = new Tuple();
			while (someFileStillHasRows)
			{
				Datum min;
				int minIndex;

				row = filerows.get(0);
				if (row!=null) {
					min = row.get(attribute);
					minIndex = 0;
				}
				else {
					min = null;
					minIndex = -1;
				}

				// check which one is min
				for(int i=1; i<filerows.size(); i++)
				{
					row = filerows.get(i);
					if(row != null)
					{
						if (min == null) 
						{
							
							min = row.get(attribute);
							minIndex = i;
						}
						else
						{
							if(check(row.get(attribute),min))
							{
								
								//min = filerows.get(i).get(attribute);
								min = row.get(attribute);
								minIndex = i;
							}
						}
					}
				}

				if (minIndex < 0) {
					someFileStillHasRows=false;
				}
				else
				{
					// write to the sorted file
					bw.append(flattenArray(filerows.get(minIndex),"|")+"\n");
					result.add(filerows.get(minIndex));
					// get another row from the file that had the min
					String line = mergefbr.get(minIndex).readLine();
					if (line != null)
					{
						filerows.set(minIndex,getMap(line.split("\\|")));
					}
					else 
					{
						filerows.set(minIndex,null);
					}
				}								
				// check if one still has rows
				/*for(int i=0; i<filerows.size(); i++)
				{

					someFileStillHasRows = false;
					if(filerows.get(i)!=null) 
					{
						if (minIndex < 0) 
						{
							System.out.println("mindex lt 0 and found row not null" + flattenArray(filerows.get(i)," "));
							System.exit(-1);
						}
						someFileStillHasRows = true;
						break;
					}
				}*/
				
				checkForData(filerows,minIndex);
				// check the actual files one more time
				addData(filerows,mergefbr);
				/*if (!someFileStillHasRows)
				{

					//write the last one not covered above
					for(int i=0; i<filerows.size(); i++)
					{
						if (filerows.get(i) == null)
						{
							String line = mergefbr.get(i).readLine();
							if (line!=null) 
							{

								someFileStillHasRows=true;
								filerows.set(i,getMap(line.split("\\|")));
							}
						}

					}
				}*/

			}



			// close all the files
			bw.close();
			fw.close();
			for(int i=0; i<mergefbr.size(); i++)
				mergefbr.get(i).close();
			for(int i=0; i<mergefr.size(); i++)
				mergefr.get(i).close();


			
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			System.exit(-1);
		}
		return result;
	}
	
	private void checkForData(ArrayList<Tuple> arr,int index)
	{
		for(int i=0; i<arr.size(); i++)
		{

			someFileStillHasRows = false;
			if(arr.get(i)!=null) 
			{
				if (index < 0) 
				{
					//System.out.println("mindex lt 0 and found row not null" + flattenArray(filerows.get(i)," "));
					System.exit(-1);
				}
				someFileStillHasRows = true;
				break;
			}
		}
	}
	
	private void addData(ArrayList<Tuple> arr,ArrayList<BufferedReader> mergefbr) throws IOException
	{
		if (!someFileStillHasRows)
		{

			//write the last one not covered above
			for(int i=0; i<arr.size(); i++)
			{
				if (arr.get(i) == null)
				{
					String line = mergefbr.get(i).readLine();
					if (line!=null) 
					{

						someFileStillHasRows=true;
						arr.set(i,getMap(line.split("\\|")));
					}
				}

			}
		}
	}

	private static Tuple getMap(String[] temp)
	{
		/*Tuple a = new Tuple();
		Iterator it=columns.iterator();
		int i=0;
		
        while(it.hasNext())
        {
        	
        	a.put(it.next().toString(),new GetString(temp[i]));
        	i++;
        }
		return a;
		*/
		Tuple a = new Tuple();
		int i=0;
		

		while(i < header.size())
		{
			if(type.get(i).equals("int"))
			{
				//System.out.println(list.get(i)+" "+type.get(i)+" "+temp[i]);
				a.put(header.get(i),new GetInteger(temp[i]));
				i++;
	    		continue;
			}
			if(type.get(i).equals("string"))
			{
				//System.out.println(list.get(i)+" "+type.get(i)+" "+temp[i]);
				a.put(header.get(i),new GetString(temp[i]));
				i++;
	    		continue;
			}
			if(type.get(i).equals("double"))
			{
				//System.out.println(list.get(i)+" "+type.get(i)+" "+temp[i]);
				a.put(header.get(i),new GetDouble(temp[i]));
				i++;
	    		continue;
			}
			if(type.get(i).equals("date"))
			{
				//System.out.println(list.get(i)+" "+type.get(i)+" "+temp[i]);
				/*SimpleDateFormat out=new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyy");
	    		SimpleDateFormat out1=new SimpleDateFormat("yyyy-MM-dd");
	    		try {
					Date date1 = out.parse(temp[i]);
					String date2 = out1.format(date1);
					a.put(header.get(i),new GetDate(date2));
					i++;
		    		continue;
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				a.put(header.get(i),new GetDate(temp[i]));
				i++;
	    		continue;
			}
		}
		
		return a;
		
	}
	
	// merge the the results for mergeSort back together 
	private static ArrayList<Tuple> merge(ArrayList<Tuple> left, ArrayList<Tuple> right)
	{
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		while (left.size() > 0 && right.size() > 0)
		{
			if(check(left.get(0).get(attribute),(right.get(0).get(attribute))))
			{
				result.add(left.get(0));
				left.remove(0);
			}
			else
			{
				result.add(right.get(0));
				right.remove(0);
			}
		}
		if (left.size()>0) 
		{
			for(int i=0; i<left.size(); i++)
				result.add(left.get(i));
		}
		if (right.size()>0) 
		{
			for(int i=0; i<right.size(); i++)
				result.add(right.get(i));
		}
		return result;
	}

	private static boolean check(Datum a, Datum b)
	{
		int t = a.compareTo(b);
		if(sortBy.equals("asc"))
		{
			if(t<=0)
			{
				return true;
			}
		}
		if(sortBy.equalsIgnoreCase("desc"))
		{
			if(t>=0)
				return true;
		}
		return false;
	}
	// just a utility function to turn arrays into strings with spaces between each element 
	private static String flattenArray(Map<String, Datum>  arr, String delimiter)
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


}
