package edu.buffalo.cse562;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class RowSerializer implements Serializer<Tuple>
{
	private ArrayList<String> colType;
	private ArrayList<String> colName;
	private Datum data;
	
	public RowSerializer(ArrayList<String> colType,ArrayList<String> colName) 
	{
		this.colType = colType;
		this.colName = colName;
	}

	@Override
	public Tuple deserialize(SerializerInput arg0) throws IOException,ClassNotFoundException 
	{
		// TODO Auto-generated method stub
		Tuple a = new Tuple();
		
		for(int i=0;i<colType.size();i++)
		{
			String type = colType.get(i);
			String name = colName.get(i);
			if(type.equals("int"))
			{
				GetInteger data;
				data = new GetInteger(arg0.readInt()+"");
				a.put(name, data);
			}

			if(type.equals("double") || type.equals("decimal"))
			{
				data = new GetDouble(arg0.readDouble()+"");
				a.put(name, data);
			}

			if(type.equals("date"))
			{
				data = new GetDate(arg0.readUTF());
				a.put(name, data);
			}

			if(type.equals("string") || type.contains("char"))
			{
				data = new GetString(arg0.readUTF());
				a.put(name, data);
			}
		}
		return a;
	}

	@Override
	public void serialize(SerializerOutput out, Tuple tp) throws IOException
	{

		for(int i=0;i<colType.size();i++)
		{
			String type = colType.get(i);
			String name = colName.get(i);
			if(type.equals("int"))
			{
				out.writeInt((int)tp.get(name).getValue());
			}

			if(type.equals("double") || type.equals("decimal"))
			{
				out.writeDouble((double)tp.get(name).getValue());
			}

			if(type.equals("date"))
			{
				out.writeUTF(tp.get(name).getValue().toString());
			}

			if(type.equals("string") || type.contains("char"))
			{
				out.writeUTF(tp.get(name).getValue().toString());
			}
		}
	}

}
