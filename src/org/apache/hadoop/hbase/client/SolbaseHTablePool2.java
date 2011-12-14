package org.apache.hadoop.hbase.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

public class SolbaseHTablePool2 {
	private final Configuration config;
	private final int maxActive;
	private final HTableInterfaceFactory tableFactory;
	
	private final ConcurrentMap<String, GenericObjectPool> tablePools = new ConcurrentHashMap<String, GenericObjectPool>();
	
	private static ConcurrentMap<String, AtomicInteger> htableCounters = new ConcurrentHashMap<String, AtomicInteger>();
	
	public SolbaseHTablePool2(final Configuration config, final int maxSize) {
		this(config, maxSize, null);
	}

	public SolbaseHTablePool2(final Configuration config, final int maxSize, final HTableInterfaceFactory tableFactory) {
		// Make a new configuration instance so I can safely cleanup when
		// done with the pool.
		this.config = config == null ? new Configuration() : new Configuration(config);
		this.maxActive = maxSize;
		this.tableFactory = tableFactory == null ? new HTableFactory() : tableFactory;
	}
	
	public HTableInterface getTable(String tableName) {
		GenericObjectPool pool = this.tablePools.get(tableName);
		
		if(pool == null){
			// lazy initialization of pool
			PoolableObjectFactory factory = new SolbaseHTableInterfaceFactory(config, tableName);
			pool = new GenericObjectPool(factory, this.maxActive);
		}
		try {
			HTableInterface table = (HTableInterface) pool.borrowObject();
			return table;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void putTable(HTableInterface table) {
		String tableName = Bytes.toString(table.getTableName());
		GenericObjectPool pool = this.tablePools.get(tableName);
		try {
			pool.returnObject(table);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
