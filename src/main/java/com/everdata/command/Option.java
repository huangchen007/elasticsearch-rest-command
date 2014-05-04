package com.everdata.command;

public class Option {
	public final static int COUNTFIELD = 1;
    public final static int LIMIT = 2;
    public final static int OTHERSTR = 3;
    public final static int PERCENTFIELD = 4;
    public final static int SHOWCOUNT = 5;
    public final static int SHOWPERC = 6;
    public final static int USEOTHER = 7;
    public final static int TIMEFORMAT = 8;
    public final static int STARTTIME = 9;
    public final static int ENDTIME = 10;
    public final static int EARLIEST = 11;
    public final static int LATEST = 12;
    public final static int SOURCETYPE = 13;
    public final static int INDEX = 14;
    public final static int HASPARENT = 15;
    public final static int HASCHILD = 16;
    public final static int MINCOUNT = 17;
	
    public int type;
	public String value;

	public Option() {
	}

}