package org.apache.hadoop.hbase.client;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

public class SolbaseHTableInterfaceFactory extends HTablePool implements PoolableObjectFactory{
	private final Configuration config;
	private String tableName = null;
	
	public SolbaseHTableInterfaceFactory(Configuration config, String tableName){
		this.config = config;
		this.tableName = tableName;
	}
	
	@Override
	public void activateObject(Object arg0) throws Exception {
		// what do I do to reinitialize HTableInterface?
	}

	@Override
	public void destroyObject(Object arg0) throws Exception {
		HTableInterface table = (HTableInterface) arg0;
		table.close();
	}

	@Override
	public Object makeObject() throws Exception {
		return this.createHTable(this.tableName);
	}

	@Override
	public void passivateObject(Object arg0) throws Exception {
		// what do I do to reinitialize HTableInterface?
		
	}

	@Override
	public boolean validateObject(Object arg0) {
		HTableInterface table = (HTableInterface) arg0;
		return Bytes.toString(table.getTableName()).equals(this.tableName);
	}

}
