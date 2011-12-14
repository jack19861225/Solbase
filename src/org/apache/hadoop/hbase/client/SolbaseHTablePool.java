package org.apache.hadoop.hbase.client;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class SolbaseHTablePool extends HTablePool {
	private final static Logger logger = Logger.getLogger(SolbaseHTablePool.class);
	
	private final ConcurrentMap<String, BlockingQueue<HTableInterface>> busyTables = new ConcurrentHashMap<String, BlockingQueue<HTableInterface>>();
	private final ConcurrentMap<String, BlockingQueue<HTableInterface>> idleTables = new ConcurrentHashMap<String, BlockingQueue<HTableInterface>>();
	
	private final Configuration config;
	private final int maxBusy;
	private final int maxIdle;
	private final HTableInterfaceFactory tableFactory;
    
	private static ConcurrentMap<String, AtomicInteger> htableCounters = new ConcurrentHashMap<String, AtomicInteger>();
	
	private static int maxWait = 5000; // max wait for pool is 5 seconds
	public SolbaseHTablePool(final Configuration config, final int maxSize) {
		this(config, maxSize, null);
	}

	public SolbaseHTablePool(final Configuration config, final int maxSize, final HTableInterfaceFactory tableFactory) {
		// Make a new configuration instance so I can safely cleanup when
		// done with the pool.
		this.config = config == null ? new Configuration() : new Configuration(config);
		this.maxBusy = maxSize;
		this.maxIdle = maxSize;
		this.tableFactory = tableFactory == null ? new HTableFactory() : tableFactory;
	}

	public HTableInterface getTable(String tableName) {
		BlockingQueue<HTableInterface> idleQueue = idleTables.get(tableName);
		BlockingQueue<HTableInterface> busyQueue = busyTables.get(tableName);

		AtomicInteger htablecounter = htableCounters.get(tableName);
		
		if(busyQueue == null){
			logger.info("creating new busy queue for table: " + tableName);
			
			// lazy initialization of this pool
			busyQueue = new ArrayBlockingQueue<HTableInterface>(this.maxBusy,false);
			busyTables.putIfAbsent(tableName, busyQueue);
		}
		
		if (idleQueue == null) {
			logger.info("creating new idle queue for table: " + tableName);
			
			// lazy initialization of this pool
			idleQueue = new ArrayBlockingQueue<HTableInterface>(this.maxIdle,true);
			idleTables.putIfAbsent(tableName, idleQueue);
			HTableInterface htable = createHTable(tableName);
			htablecounter = new AtomicInteger(1);
			htableCounters.putIfAbsent(tableName, htablecounter);
			
			logger.info("counter for total htable created for this table: " + tableName + " count: " + htablecounter.get());
			try {
				busyQueue.put(htable);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
			return htable;
		}
		
		HTableInterface table;
		synchronized (idleQueue) {
			table = idleQueue.poll();
		}
		
        //get the current time stamp
        long now = System.currentTimeMillis();
		
		while(true){
			if(table != null){
				try {
					busyQueue.put(table);
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new RuntimeException(e.getMessage());
				}
				return table;
			}
			
            
            //if we get here, see if we need to create one
            //this is not 100% accurate since it doesn't use a shared
            //atomic variable - a connection can become idle while we are creating 
            //a new connection
            if (htablecounter.get() < this.maxBusy) {
                //atomic duplicate check
                if (htablecounter.addAndGet(1) > this.maxBusy) {
                    //if we got here, two threads passed through the first if
                    htablecounter.decrementAndGet();
                    logger.info("trying to acquire resource and lost battle because pool is full for time being: " + tableName);
                } else {
    				table = createHTable(tableName);
    				logger.info("counter for total htable created for this table: " + tableName + " count: " + htablecounter.get());
    				try {
    					busyQueue.put(table);
    				} catch (InterruptedException e) {
    					e.printStackTrace();
    					throw new RuntimeException(e.getMessage());
    				}
    				return table;
                }
            } //end if
			
			try {
				// TODO: pass in wait value from param or config
				table = idleQueue.poll(100,TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
			
			//we didn't get a connection, lets see if we timed out
            if (table == null) {
                if ((System.currentTimeMillis() - now) >= maxWait) {
                    throw new RuntimeException("[" + Thread.currentThread().getName()+"] " +
                        "Timeout: Pool empty. Unable to fetch a connection in " + (maxWait / 1000) +
                        " seconds, none available["+busyQueue.size()+" in use] for tableName: " + tableName);
                } else {
                    //no timeout, lets try again
                    continue;
                }
            }
		}
	}

	public void putTable(HTableInterface table) {
		String tableName = Bytes.toString(table.getTableName());
		BlockingQueue<HTableInterface> idleQueue = idleTables.get(tableName);
		BlockingQueue<HTableInterface> busyQueue = busyTables.get(tableName);

		synchronized (idleQueue) {
			if (idleQueue.size() >= maxIdle)
				return;
			
			// remove from busy queue, and put it back to idle queue
			if(busyQueue.remove(table)){
				idleQueue.add(table);
			}
		}
	}

	public void closeTablePool(final String tableName) {
		// clean up busy tables
		Queue<HTableInterface> busyQueue = busyTables.get(tableName);
		synchronized (busyQueue) {
			HTableInterface table = busyQueue.poll();
			while (table != null) {
				this.tableFactory.releaseHTableInterface(table);
				table = busyQueue.poll();
			}
		}

		// clean up idle tables
		Queue<HTableInterface> idleQueue = idleTables.get(tableName);
		synchronized (idleQueue) {
			HTableInterface table = idleQueue.poll();
			while (table != null) {
				this.tableFactory.releaseHTableInterface(table);
				table = idleQueue.poll();
			}
		}
		
		HConnectionManager.deleteConnection(this.config, true);
	}

	int getCurrentPoolSize(String tableName) {
		Queue<HTableInterface> queue = busyTables.get(tableName);
		synchronized (queue) {
			return queue.size();
		}
	}

}
