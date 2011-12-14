package org.solbase.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

public abstract class VersionedCache<K, V, Z> {
	
	private static final Logger log = Logger.getLogger(VersionedCache.class);
	
	private AtomicInteger hitCount = new AtomicInteger(0);
	private AtomicInteger count = new AtomicInteger(0);
	
	protected Long timeout;
	
	
	
	protected abstract CachedObjectWrapper<V, Z> getInternal(K key)  throws IOException;
	public abstract void put(K key, CachedObjectWrapper<V, Z> aValue) throws IOException;
	public abstract void clear() throws IOException;
	public boolean isCacheFull() { return false; }
	
	public CachedObjectWrapper<V, Z> get(K key)  throws IOException {
		count.incrementAndGet();
		CachedObjectWrapper<V, Z> tmp = getInternal(key);
		
		if (tmp != null) {
			hitCount.incrementAndGet();
			if (count.intValue() % 100 == 0) {
				log.debug("Cache hit rate: " + (hitCount.intValue() * 100)/count.intValue() + "% "+ this.getClass().getSimpleName() + "/" + tmp.getValue().getClass().getSimpleName());
			}
		}
		
		return tmp;
	}

	
	public void setTimeout(Long timeout) {
		this.timeout = timeout;
	}
	
	public void resetCacheTime(K key, Long time) throws IOException {
		CachedObjectWrapper<V, Z> cow = getInternal(key);
		if (cow != null) {
			cow.setCacheTime(time);
		}
	}
	
	protected byte[] serialize(V o) {
		if (o == null) {
			throw new NullPointerException("Can't serialize null");
		}
		byte[] rv = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);
			os.writeObject(o);
			os.close();
			bos.close();
			rv = bos.toByteArray();
		} catch (IOException e) {
			throw new IllegalArgumentException("Non-serializable object", e);
		}
		return rv;
	}

	@SuppressWarnings("unchecked")
	protected V deserialize(byte[] in) {
		Object rv = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream is = null;
		try {
			if (in != null) {
				bis = new ByteArrayInputStream(in);
				is = new ObjectInputStream(bis);
				rv = is.readObject();

			}
		} catch (IOException e) {
			log.error("Caught IOException decoding " + in.length + " bytes of data", e);
		} catch (ClassNotFoundException e) {
			log.error("Caught CNFE decoding " + in.length + " bytes of data", e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					// ignore
				}
			}
			if (bis != null) {
				try {
					bis.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
		return (V) rv;
	}


	
	
	protected static final byte[] compress(byte[] in) {
		if (in == null) {
			throw new NullPointerException("Can't compress null");
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		GZIPOutputStream gz = null;
		try {
			gz = new GZIPOutputStream(bos);
			gz.write(in);
		} catch (IOException e) {
			throw new RuntimeException("IO exception compressing data", e);
		} finally {
			if (gz != null) {
				try {
					gz.close();
				} catch (IOException e) {
					log.error("Close GZIPOutputStream error", e);
				}
			}
			if (bos != null) {
				try {
					bos.close();
				} catch (IOException e) {
					log.error("Close ByteArrayOutputStream error", e);
				}
			}
		}
		byte[] rv = bos.toByteArray();
		// log.debug("Compressed %d bytes to %d", in.length, rv.length);
		return rv;
	}

	/**
	 * Decompress the given array of bytes.
	 * 
	 * @return null if the bytes cannot be decompressed
	 */
	protected static byte[] decompress(byte[] in) {
		ByteArrayOutputStream bos = null;
		if (in != null) {
			ByteArrayInputStream bis = new ByteArrayInputStream(in);
			bos = new ByteArrayOutputStream();
			GZIPInputStream gis = null;
			try {
				gis = new GZIPInputStream(bis);

				byte[] buf = new byte[16 * 1024];
				int r = -1;
				while ((r = gis.read(buf)) > 0) {
					bos.write(buf, 0, r);
				}
			} catch (IOException e) {
				log.error("Failed to decompress data", e);
				bos = null;
			} finally {
				if (gis != null) {
					try {
						gis.close();
					} catch (IOException e) {
						log.error("Close GZIPInputStream error", e);
					}
				}
				if (bis != null) {
					try {
						bis.close();
					} catch (IOException e) {
						log.error("Close ByteArrayInputStream error", e);
					}
				}
			}
		}
		return bos == null ? null : bos.toByteArray();
	}
}
