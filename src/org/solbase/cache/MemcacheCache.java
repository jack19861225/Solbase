package org.solbase.cache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.UUID;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.transcoders.SerializingTranscoder;
import net.rubyeye.xmemcached.utils.AddrUtil;

public class MemcacheCache<K, V, Z> extends VersionedCache<K, V, Z> {

	/*
	 * static Process memcacheProcess; static { try { memcacheProcess =
	 * Runtime.getRuntime().exec("/opt/local/bin/memcached"); Thread
	 * closeChildThread = new Thread() { public void run() {
	 * memcacheProcess.destroy(); } };
	 * 
	 * Runtime.getRuntime().addShutdownHook(closeChildThread);
	 * 
	 * } catch (IOException e) { e.printStackTrace(); } }
	 */

	private static final int memcacheSize = (1024 * 1024) - 1024;
	private MemcachedClient memcachedClient = null;

	public MemcacheCache() {
		this(null, null);
	}

	public MemcacheCache(String hostName, Integer port) {
		String memcacheHostName;
		String memcachePort;

		if (hostName == null) {
			memcacheHostName = System.getProperty("solbase.memcache.hostname");
			if (memcacheHostName == null && ResourceBundle.getBundle("solbase") != null) {
				memcacheHostName = ResourceBundle.getBundle("solbase").getString("memcache.hostname");
			}

			if (memcacheHostName == null) {
				memcacheHostName = "localhost";
			}
		} else {
			memcacheHostName = hostName;
		}

		if (hostName == null) {
			memcachePort = System.getProperty("solbase.memcache.port");

			if (memcachePort == null && ResourceBundle.getBundle("solbase") != null) {
				memcachePort = ResourceBundle.getBundle("solbase").getString("memcache.port");
			}

			if (memcachePort == null) {
				memcachePort = "11211";
			}
		} else {
			memcachePort = port.toString();
		}

		MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(memcacheHostName + ":" + memcachePort));
		builder.setCommandFactory(new BinaryCommandFactory());
		builder.setConnectionPoolSize(100);

		try {
			memcachedClient = builder.build();
			memcachedClient.setOpTimeout(30000);
			SerializingTranscoder st = new SerializingTranscoder(memcacheSize);
			memcachedClient.setTranscoder(st);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void resetCacheTime(K key, Long time) throws IOException {
		MemcacheCachedObjectWrapper<V, Z> value = getInternal(key);
		value.setCacheTime(time);
		try {
			memcachedClient.set(key.toString() + key.getClass().getName().hashCode(), 0, value);
		} catch (Exception ex) {
			throw new IOException(ex);
		}
	}

	protected MemcacheCachedObjectWrapper<V, Z> getInternal(K key) throws IOException {
		try {
			MemcacheCachedObjectWrapper<V, Z> tmp = memcachedClient.get(key.toString() + key.getClass().getName().hashCode());
			if (tmp != null) {
				if (tmp.getUuids() == null) {
					tmp.setValue(deserialize(tmp.getValueBytes()));
					tmp.setValueBytes(null);
				} else {
					ByteArrayOutputStream baos = new ByteArrayOutputStream(tmp.getUuids().size() * memcacheSize);
					Map<String, byte[]> results = memcachedClient.get(tmp.getUuids());

					for (String uuid : tmp.getUuids()) {
						byte[] tmpBytes = results.get(uuid);
						if (tmpBytes == null) {// part is missing, clean it all
												// up
							memcachedClient.delete(key.toString() + key.getClass().getName().hashCode());
							for (String uuidToRemove : tmp.getUuids()) {
								memcachedClient.delete(uuidToRemove);
								return null;
							}
						}
						baos.write(tmpBytes);
					}

					tmp.setValue(deserialize(decompress(baos.toByteArray())));
				}
			}

			return tmp;
		} catch (Exception ex) {
			throw new IOException(ex);
		}
	}

	public void put(K key, CachedObjectWrapper<V, Z> aValue) throws IOException {
		try {
			MemcacheCachedObjectWrapper<V, Z> value = new MemcacheCachedObjectWrapper<V, Z>(aValue.getValue(), aValue.getVersionIdentifier(), aValue.getCacheTime());
			byte[] serializedBytes = serialize(value.getValue());

			if (serializedBytes.length > ((memcacheSize) - 1000)) {
				ArrayList<String> uuids = new ArrayList<String>(serializedBytes.length / memcacheSize);
				ByteBuffer bb = ByteBuffer.wrap(compress(serializedBytes));
				int remaining = 0;
				while ((remaining = bb.remaining()) > 0) {
					byte[] chunk = new byte[remaining < memcacheSize ? remaining : memcacheSize];
					bb.get(chunk);
					String uuid = UUID.randomUUID().toString();
					uuids.add(uuid);
					memcachedClient.set(uuid, 0, chunk);
				}

				value.setUuids(uuids);
			} else {
				value.setValueBytes(serializedBytes);
			}
			value.setValue(null);
			memcachedClient.set(key.toString() + key.getClass().getName().hashCode(), 0, value);
		} catch (Exception ex) {
			throw new IOException(ex);
		}
	}

	public void clear() throws IOException {
		try {
			memcachedClient.flushAll();
		} catch (Exception ex) {
			throw new IOException(ex);
		}
	}
}
