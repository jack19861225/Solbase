package org.solbase.indexer;

import java.util.ResourceBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class SolbaseIndexer {
	public static void main(String[] args) throws Exception{
		SolbaseIndexUtil indexUtil = null;
		// set SolbaseIndexUtil
		if(ResourceBundle.getBundle("solbase") != null){
			String className = ResourceBundle.getBundle("solbase").getString("class.solbaseIndexUtil");
			if(className != null){
				try {
					indexUtil = (SolbaseIndexUtil) Class.forName(className).newInstance();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		if(args.length != 1){
			System.out.println("Usage: hadoop jar solbase.jar org.solbase.indexer.SolbaseIndexer <table name>");
			System.exit(-1);
		}
		
		if(indexUtil != null){
			int errCode = ToolRunner.run(new Configuration(), new SolbaseIndexerTool(indexUtil), args);
			System.exit(errCode);
		} else {
			System.exit(-1);
		}
	}
}


