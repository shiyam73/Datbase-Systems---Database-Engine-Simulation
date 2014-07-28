package edu.buffalo.cse562;

import java.io.Serializable;

public interface Datum <T> extends Serializable{
	public T getValue();
    public String getType();
    public int compareTo(Datum d);
    
}
