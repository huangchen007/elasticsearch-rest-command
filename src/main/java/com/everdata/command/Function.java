package com.everdata.command;

public class Function {
	public final static int SUM = 1;
	public final static int COUNT = 2;
	public final static int DC = 3;
	public final static int AVG = 4;
	public final static int MAX = 5;
	public final static int MIN = 6;
	
	public int type;
	public int fieldtype; //SCRIPT or SINGLE
	public String field = null;
	public String name = null;
	public String as = null;
	public int order = 0;
	public String statsField = null;
	
	
	public Function(){}
	
	
	public static String genStatField(Function func){
		if(func.as != null){
			return func.as;
		}else{
			StringBuilder statField = new StringBuilder(func.name);
			statField.append("-");
			statField.append(func.field);
			
			return statField.toString();
		}
	}
}
