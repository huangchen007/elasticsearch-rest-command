package com.everdata.command;

import java.util.ArrayList;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import com.everdata.parser.AST_Search;
import com.everdata.parser.AST_Start;
import com.everdata.parser.CommandParserTreeConstants;
import com.everdata.parser.Node;

public class Optimizer implements CommandParserTreeConstants {
	
	int showFrom = 0; 
	int showSize = 10;
	
	public void setShow(int from, int size){
		this.showFrom = from;
		this.showSize = size;
	}
	
//	public void setClient(Client client){
//		this.client = client;
//	}
//	
//	public void setLogger(ESLogger logger){
//		this.logger = logger;
//	}
	
	static public Plan[] evaluate(AST_Start tree, Client client,ESLogger logger) throws CommandException {
		
		ArrayList<Plan> al = new ArrayList<Plan>();
		
		for (Node n : tree.getChildren()) {
			if (n instanceof AST_Search) {
		//		al.add(new Search((AST_Search)n, client, logger));
			}
		}
		
		
		return al.toArray(new Plan[al.size()]);

		// dispatch optimizing to the individual classes
		/*
		 * for (Node n : ((SimpleNode)tree)) { case JJT_TOP: //return new
		 * Top((AST_Top) tree.getStmt());
		 * 
		 * case JJT_CREATEINDEX: // System.out.println("Creating index...");
		 * return new CreateIndex((AST_CreateIndex) tree.getStmt());
		 * 
		 * case JJT_CREATETABLE: // System.out.println("Creating table...");
		 * return new CreateTable((AST_CreateTable) tree.getStmt());
		 * 
		 * case JJT_DROPINDEX: // System.out.println("Dropping Index...");
		 * return new DropIndex((AST_DropIndex) tree.getStmt());
		 * 
		 * case JJT_DROPTABLE: // System.out.println("Dropping Table...");
		 * return new DropTable((AST_DropTable) tree.getStmt());
		 * 
		 * case JJT_DESCRIBE: // System.out.println("Describing..."); return new
		 * Describe((AST_Describe) tree.getStmt());
		 * 
		 * case JJT_INSERT: // System.out.println("Inserting..."); return new
		 * Insert((AST_Insert) tree.getStmt());
		 * 
		 * case JJT_SELECT: // System.out.println("Selecting..."); return new
		 * Select((AST_Select) tree.getStmt());
		 * 
		 * case JJT_UPDATE: // System.out.println("Updating..."); return new
		 * Update((AST_Update) tree.getStmt());
		 * 
		 * case JJT_DELETE: // System.out.println("Deleting..."); return new
		 * Delete((AST_Delete) tree.getStmt());
		 * 
		 * default: throw new CommandException("unsupported query type");
		 * 
		 * } // switch
		 */
	} // public static Plan evaluate(AST_Start tree) throws QueryException

} // public class Optimizer implements EsCmdTreeConstants
