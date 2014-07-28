package edu.buffalo.cse562;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

public class GetComparator implements Comparator<Map<String, Datum>>{
	private String key=null;
	private String order=null;

	public GetComparator(String key1, String order1)
	{
		order=order1;
		key=key1;
	}

	@Override
	public int compare(Map<String, Datum> first, Map<String, Datum> second) {

		Object secondvalue;
		Object firstvalue;

		
		

		Datum tempF = first.get(key);
		
		//System.out.println(first.get(key).getValue());
		Datum tempS = second.get(key);
		
	
		if(order.equalsIgnoreCase("ASC"))
			return tempF.compareTo(tempS);
		else
			return (tempF.compareTo(tempS)*-1);


	}

}