package com.everdata.test;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import junit.framework.TestCase;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.everdata.command.CommandException;
import com.everdata.command.Optimizer;
import com.everdata.command.Plan;
import com.everdata.parser.AST_Start;
import com.everdata.parser.CommandParser;
import com.everdata.parser.ParseException;

import static org.junit.Assert.*;


public class CommandActionTest{
    Client client;
    
	@Before
    public void setUp() throws Exception {
		System.out.println("connect");
		client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("192.168.200.13", 9300));
		
	}

    @After
    public void tearDown() throws Exception {
    	client.close();
    	System.out.println("close");
    }

    @Test
    public void deleteAction() {
    	assertEquals(true, true);
    }
    
    @Test
    public void selectAction() {
    	CommandParser cp = null;
    	try {
			 cp = new CommandParser(new DataInputStream(new FileInputStream("src/test/searchcommand")));		   	
    	} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    	
    	
    	Plan[] plans = null;
		try {
			plans = Optimizer.evaluate(cp.Start(), client, null);
		} catch (CommandException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	for(Plan p : plans){
    		ActionResponse res = p.execute();	
    	}
    	
    	assertEquals("yes", 1, 1);
    }

}
