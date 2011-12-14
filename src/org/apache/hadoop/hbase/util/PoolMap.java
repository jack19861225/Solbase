/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * The <code>PoolMap</code> maps a key to a collection of values, the elements
 * of which are managed by a pool. In effect, that collection acts as a shared
 * pool of resources, access to which is closely controlled as per the semantics
 * of the pool.
 * 
 * <p>
 * In case the size of the pool is set to a non-zero positive number, that is
 * used to cap the number of resources that a pool may contain for any given
 * key. A size of {@link Integer#MAX_VALUE} is interpreted as an unbounded pool.
 * </p>
 * 
 * @param <K>
 *            the type of the key to the resource
 * @param <V>
 *            the type of the resource being pooled
 * 
 * @author Karthick Sankarachary
 */
public class PoolMap<K, V> implements Map<K, V> {
	 private static final Log LOG =
		    LogFactory.getLog("org.apache.hadoop.hbase.util.PoolMap");
	 
	private PoolType poolType;

	private int poolMaxSize;

	private Map<K, Pool<V>> pools = Collections.synchronizedMap(new HashMap<K, Pool<V>>());

	public PoolMap(PoolType poolType, int poolMaxSize) {
		this.poolType = poolType;
		this.poolMaxSize = poolMaxSize;
	}

	@Override
	public V get(Object key) {
		Pool<V> pool = pools.get(key);
		
		return pool != null ? pool.get() : null;
	}

	@Override
	public V put(K key, V value) {
		Pool<V> pool = pools.get(key);
		if (pool == null) {
			synchronized (this) {
				pool = pools.get(key);
				if (pool == null) {
					pools.put(key, pool = createPool());
				}
			}
		}
		
		return pool != null ? pool.put(value) : null;
	}

	@Override
	public V remove(Object key) {
		Pool<V> pool = pools.remove(key);
		if (pool != null) {
			pool.clear();
		}
		return null;
	}

	public boolean remove(K key, V value) {
		Pool<V> pool = pools.get(key);
		return pool != null ? pool.remove(value) : false;
	}

	@Override
	public Collection<V> values() {
		Collection<V> values = new ArrayList<V>();
		for (Pool<V> pool : pools.values()) {
			Collection<V> poolValues = pool.values();
			if (poolValues != null) {
				values.addAll(poolValues);
			}
		}
		return values;
	}

	@Override
	public boolean isEmpty() {
		return pools.isEmpty();
	}

	@Override
	public int size() {
		return pools.size();
	}

	public int size(K key) {
		Pool<V> pool = pools.get(key);
		return pool != null ? pool.size() : 0;
	}

	@Override
	public boolean containsKey(Object key) {
		return pools.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		if (value == null) {
			return false;
		}
		for (Pool<V> pool : pools.values()) {
			if (value.equals(pool.get())) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void clear() {
		for (Pool<V> pool : pools.values()) {
			pool.clear();
		}
		pools.clear();
	}

	@Override
	public Set<K> keySet() {
		return pools.keySet();
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		Set<Map.Entry<K, V>> entries = new HashSet<Entry<K, V>>();
		for (Map.Entry<K, Pool<V>> poolEntry : pools.entrySet()) {
			final K poolKey = poolEntry.getKey();
			final Pool<V> pool = poolEntry.getValue();
			for (final V poolValue : pool.values()) {
				if (pool != null) {
					entries.add(new Map.Entry<K, V>() {
						@Override
						public K getKey() {
							return poolKey;
						}

						@Override
						public V getValue() {
							return poolValue;
						}

						@Override
						public V setValue(V value) {
							return pool.put(value);
						}
					});
				}
			}
		}
		return entries;
	}

	protected interface Pool<R> {
		public R get();

		public R put(R resource);

		public boolean remove(R resource);

		public void clear();

		public Collection<R> values();

		public int size();
	}

	public enum PoolType {
		Reusable("reusable"), ThreadLocal("threadlocal"), RoundRobin("roundrobin");

		private String configName;

		PoolType(String configName) {
			this.configName = configName;
		}

		public String getConfigName() {
			return configName;
		}

		public static PoolType valueOf(String poolTypeName, PoolType defaultPoolType, PoolType... otherPoolTypes) {
			PoolType poolType = null;
			if (poolTypeName != null)
				poolType = PoolType.valueOf(poolTypeName);
			if (poolType != null) {
				boolean allowedType = false;
				if (poolType.equals(defaultPoolType)) {
					allowedType = true;
				} else {
					if (otherPoolTypes != null) {
						for (PoolType allowedPoolType : otherPoolTypes) {
							if (poolType.equals(allowedPoolType)) {
								allowedType = true;
								break;
							}
						}
					}
				}
				if (!allowedType) {
					poolType = null;
				}
			}
			return (poolType != null) ? poolType : defaultPoolType;
		}
	}

	protected Pool<V> createPool() {
		switch (poolType) {
		case Reusable:
			return new ReusablePool<V>(poolMaxSize);
		case RoundRobin:
			return new RoundRobinPool<V>(poolMaxSize);
		case ThreadLocal:
			return new ThreadLocalPool<V>(poolMaxSize);
		}
		return null;
	}
}
