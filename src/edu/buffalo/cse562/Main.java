package edu.buffalo.cse562;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import net.sf.jsqlparser.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

public class Main {
	
	static File dataDir; 
	static File swapDir; 
	static File indexDir; 
	static File sqlFile;
	static int build = 0;
	
	public static void main(String args[]) throws Exception{
		long lStartTime = new Date().getTime(); // start time
		StatementEvaluator eval = null ;
		for(int i=0;i<args.length;i++)
		{
			if(args[i].equals("--data"))
			{
				dataDir = new File(args[i+1]);
				eval = new StatementEvaluator(dataDir.getAbsolutePath());
				if(!dataDir.isDirectory())
				{
					throw new Exception("Directory Path is wrong");
				}
				i++;
			}
			else if(args[i].equals("--swap"))
			{
				swapDir = new File(args[i+1]);
				if(!dataDir.isDirectory())
				{
					throw new Exception("Directory Path is wrong");
				}
				i++;
			}
			else if(args[i].equals("--index"))
			{
				indexDir = new File(args[i+1]);
				if(!indexDir.isDirectory())
				{
					throw new Exception("Directory Path is wrong");
				}
				i++;
			}
			else if(args[i].equals("--build"))
			{
				build++;
			}
			else
			{
				sqlFile = new File(args[i]);
				
				FileReader stream = new FileReader(sqlFile);
		        CCJSqlParser parser = new CCJSqlParser(stream);
		        Statement stmt;
		        
		        while ((stmt = parser.Statement()) != null) {
		        	stmt.accept(eval);
		        }
			}
		}
		
		/*dataDir = new File(args[1]);
		if(dataDir.isDirectory())
		{
			File[] dirContents = dataDir.listFiles();
			for(File f : dirContents)
			{
				//System.out.println(f);
			}
		}
		else
		{
			throw new Exception("Directory Path is wrong");
		}
		
		File sqlFile = new File(args[2]);*/
		/*BufferedReader fileReader = new BufferedReader (new FileReader (sqlFile));
		
		CCJSqlParserManager pm = new CCJSqlParserManager();
		Statement statement = null;
		StatementEvaluator eval = new StatementEvaluator(dataDir.getAbsolutePath());	//NEED TO CHANGE
		
		
		String readLine = null;
		String temp = "";
		while((readLine = fileReader.readLine())!=null)
		{
			//ArrayList<String> stmtList = new ArrayList<String>();
			//System.out.println(readLine);
			statement = pm.parse(new StringReader(readLine));
			//System.out.println("The Statement is/are : " + statement);
			statement.accept(eval);
			if(!readLine.contains(";"))
				temp += readLine.trim()+" ";
			else
			{
				temp +=readLine.replaceAll(";","");
				System.out.println(temp);
				statement = pm.parse(new StringReader(temp));
				temp="";
				statement.accept(eval);
			}
		}
		FileReader stream = new FileReader(sqlFile);
        CCJSqlParser parser = new CCJSqlParser(stream);
        Statement stmt;
        
        while ((stmt = parser.Statement()) != null) {
        	stmt.accept(eval);
        }*/
	
		long lEndTime = new Date().getTime(); // end time
		 
		long difference = lEndTime - lStartTime; // check different
 
		long minutes = TimeUnit.MILLISECONDS.toMinutes(difference);
		long seconds = TimeUnit.MILLISECONDS.toMillis(difference);
		//System.out.println(difference);
		if(minutes > 0)
		System.out.println("Elapsed minutes: " + minutes);
		else
			System.out.println("Elapsed seconds: "+seconds);

	}
	
	public static String getDatadir()
	{
		return dataDir.getAbsolutePath();
	}

	public static String getIndexdir()
	{
		return indexDir.getAbsolutePath();
	}

}
