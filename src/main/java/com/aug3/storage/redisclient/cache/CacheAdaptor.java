package com.aug3.storage.redisclient.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aug3.storage.redisclient.JedisAdaptor;

/**
 * This class encapsulate methods for distributed cache implemented by redis
 * 
 * @author Roger.xia
 * 
 */
public class CacheAdaptor {

	private final static JedisAdaptor jedisAdaptor = new JedisAdaptor();

	public final static JedisAdaptor getCacheAdaptor() {
		return jedisAdaptor;
	}

	/**
	 * get String value by key, if not exists, build the data and store it to
	 * cache.
	 * 
	 * @param key
	 * @param placeholder
	 * @param builder
	 * @return
	 * @throws Exception
	 */
	public static String getAndSet(CacheKey key, String placeholder, AbstractDataBuilder builder) throws Exception {

		String cacheKey = null;

		if (placeholder == null || placeholder.length() == 0) {
			cacheKey = key.getKey();
		} else {
			cacheKey = key.getKey(placeholder);
		}

		String retObj = jedisAdaptor.get(cacheKey);
		if (retObj != null)
			return retObj;

		retObj = (String) builder.buildData();

		if (retObj != null) {
			jedisAdaptor.setex(cacheKey, retObj, key.getTtl());
		}

		return retObj;

	}

	/**
	 * Get Object value by key, if key not exists, build the data from data
	 * source and cache it.
	 * 
	 * Make sure your cached Object should implement Serializable.
	 * 
	 * @param key
	 * @param placeholder
	 * @param builder
	 * @return
	 * @throws Exception
	 */
	public static <T> T getAndSetObj(CacheKey key, String key_placeholder, AbstractDataBuilder<T> builder)
			throws Exception {

		return getAndSetObj(key, key_placeholder, builder, true);

	}

	/**
	 * This is a tricky one for getAndSetObj, we add a needCache flag to skip
	 * cache when needed.
	 * 
	 * @param key
	 * @param placeholder
	 * @param builder
	 * @param needCache
	 * @return
	 * @throws Exception
	 */
	public static <T> T getAndSetObj(CacheKey key, String placeholder, AbstractDataBuilder<T> builder, boolean needCache)
			throws Exception {

		if (needCache) {
			String cacheKey = null;

			if (placeholder == null || placeholder.length() == 0) {
				cacheKey = key.getKey();
			} else {
				cacheKey = key.getKey(placeholder);
			}

			T retObj = (T) jedisAdaptor.getSerializableObj(cacheKey);

			if (retObj != null)
				return retObj;

			retObj = builder.buildData();

			if (retObj != null) {
				jedisAdaptor.setexSerializableObj(cacheKey, retObj, key.getTtl());
			}
			return retObj;
		} else {
			return builder.buildData();
		}

	}

	/**
	 * get String value from hash : {cache_key:{field:string_value}}, if not
	 * exists, find it in data store and cached it.
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param field
	 * @param builder
	 * @return
	 * @throws Exception
	 */
	public static String hashGetAndSet(CacheKey key, String key_placeholder, String field,
			AbstractDataBuilder<String> builder) throws Exception {

		String cacheKey = key.getKey(key_placeholder);

		boolean exist = jedisAdaptor.exists(cacheKey);

		String value = null;

		if (exist) {
			value = jedisAdaptor.hget(cacheKey, field);

			if (value != null)
				return value;

		}

		value = builder.buildData();

		if (value != null) {
			jedisAdaptor.hset(cacheKey, field, value);
			if (!exist)
				jedisAdaptor.expire(cacheKey, key.getTtl());
		}
		return value;

	}

	/**
	 * get Class<T> type value from hash : {cache_key:{field:Class<T>}}, if not
	 * exists, find it in data store and cached it.
	 * 
	 * Make sure <T> implements Serializable.
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param field
	 * @param clz
	 * @param builder
	 * @return
	 * @throws Exception
	 */
	public static <T> T hashGetAndSetObj(CacheKey key, String key_placeholder, String field, Class<T> clz,
			AbstractDataBuilder<T> builder) throws Exception {

		String cacheKey = key.getKey(key_placeholder);

		boolean exist = jedisAdaptor.exists(cacheKey);

		T value = null;

		if (exist) {
			value = jedisAdaptor.hgetObj(cacheKey, clz, field);

			if (value != null)
				return value;

		}

		value = builder.buildData();

		if (value != null) {
			jedisAdaptor.hsetObj(cacheKey, field, value);
			if (!exist)
				jedisAdaptor.expire(cacheKey, key.getTtl());
		}
		return value;

	}

	/**
	 * get multiple object value for specified keys from cache :
	 * {cache_key:{field:Object}}, if not exists, find it in data store and
	 * cached it.
	 * 
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param fields
	 * @param typeOfValue
	 * @param builder
	 * @return Map : only the values found from cache and data store
	 * @throws Exception
	 */
	public static <T> Map<String, T> hashMGetAndSetObj(CacheKey key, String key_placeholder,
			final Collection<String> fields, Class<T> typeOfValue, HashDataBuilder<Map<String, T>> builder)
			throws Exception {

		String cacheKey = key.getKey(key_placeholder);

		boolean exist = jedisAdaptor.exists(cacheKey);

		Map<String, T> resultMap = new HashMap<String, T>();
		Set<String> left = new HashSet<String>(fields);
		if (exist) {
			resultMap.putAll(jedisAdaptor.hmgetObj(cacheKey, typeOfValue, left.toArray(new String[] {})));
			if (!resultMap.isEmpty()) {
				if (resultMap.size() == left.size()) {
					return resultMap;
				}
				left.removeAll(resultMap.keySet());
			}
		}

		Map<String, T> valueMap = builder.buildData(left);

		if (valueMap != null && !valueMap.isEmpty()) {
			resultMap.putAll(valueMap);
			jedisAdaptor.hmsetObj(cacheKey, valueMap);
			if (!exist)
				jedisAdaptor.expire(cacheKey, key.getTtl());
		}
		return resultMap;

	}

	/**
	 * get type <T> object value from hash by field: {cache_key:{field:type <T>
	 * Object}}
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param field
	 * @param clz
	 * @return
	 * @throws Exception
	 */
	public static <T> T hgetObj(CacheKey key, String key_placeholder, String field, Class<T> clz) throws Exception {

		String cacheKey = key.getKey(key_placeholder);
		if (jedisAdaptor.exists(cacheKey)) {
			return jedisAdaptor.hgetObj(cacheKey, clz, field);
		}
		return null;

	}

	/**
	 * Set type object value to hash for field: {cache_key:{field:Object}}
	 * 
	 * Make sure your object should implement Serializable.
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param field
	 * @param valueObj
	 * @throws Exception
	 */
	public static void hsetObj(CacheKey key, String key_placeholder, String field, Object valueObj) throws Exception {

		if (valueObj != null) {
			String cacheKey = key.getKey(key_placeholder);
			boolean exist = jedisAdaptor.exists(cacheKey);
			jedisAdaptor.hsetObj(cacheKey, field, valueObj);
			if (!exist)
				jedisAdaptor.expire(cacheKey, key.getTtl());
		}

	}

	/**
	 * get multiple type <T> object value from hash by fields:
	 * {cache_key:{field:type <T> Object}}
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param fields
	 * @param clz
	 * @return
	 * @throws Exception
	 */
	public static <T> Map<String, T> hmgetObj(CacheKey key, String key_placeholder, String[] fields, Class<T> clz)
			throws Exception {

		String cacheKey = key.getKey(key_placeholder);
		if (jedisAdaptor.exists(cacheKey)) {
			return jedisAdaptor.hmgetObj(cacheKey, clz, fields);
		}
		return null;

	}

	/**
	 * Set HashMap<String, ? extends Serializable> object to hash :
	 * {cache_key:{field:Object}}
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param hash
	 * @throws Exception
	 */
	public static void hmsetObj(CacheKey key, String key_placeholder, Map hash) throws Exception {

		if (hash != null) {
			String cacheKey = key.getKey(key_placeholder);
			boolean exist = jedisAdaptor.exists(cacheKey);
			jedisAdaptor.hmsetObj(cacheKey, hash);
			if (!exist)
				jedisAdaptor.expire(cacheKey, key.getTtl());
		}

	}

	/**
	 * get multiple String object value from hash by fields:
	 * {cache_key:{field:String}}
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param fields
	 * @return
	 * @throws Exception
	 */
	public static Map<String, String> hmget(CacheKey key, String key_placeholder, String[] fields) throws Exception {

		String cacheKey = key.getKey(key_placeholder);

		boolean exist = jedisAdaptor.exists(cacheKey);

		Map<String, String> hash = new HashMap<String, String>();
		List<String> list = null;
		if (exist) {
			list = jedisAdaptor.hmget(cacheKey, fields);
			if (list != null) {
				for (int i = 0; i < fields.length; i++) {
					if (list.get(i) != null)
						hash.put(fields[i], list.get(i));
				}
			}
		}
		return hash;
	}

	/**
	 * Set HashMap<String, String> object to hash : {cache_key:{field:String}}
	 * 
	 * @param key
	 * @param key_placeholder
	 * @param hash
	 * @throws Exception
	 */
	public static void hmset(CacheKey key, String key_placeholder, Map<String, String> hash) throws Exception {

		if (hash != null) {
			String cacheKey = key.getKey(key_placeholder);
			boolean exist = jedisAdaptor.exists(cacheKey);
			jedisAdaptor.hmset(cacheKey, hash);
			if (!exist)
				jedisAdaptor.expire(cacheKey, key.getTtl());
		}

	}

	/**
	 * delete fields in hashmap, if fields is null, then delete the whole key.
	 * 
	 * @param key
	 * @param fields
	 */
	public static void hashdel(String key, String... fields) {

		if (key == null) {
			return;
		}

		boolean exists = jedisAdaptor.exists(key);
		if (exists) {
			if (fields != null && fields.length > 0) {
				jedisAdaptor.hdel(key, fields);
			} else {
				jedisAdaptor.delete(key);
			}
		}

	}

	/**
	 * flush all cache key in pattern specified by key parameter.
	 * 
	 * @param key
	 * @return
	 */
	public static int flush(String key) {

		if (key == null) {
			return 0;
		}
		Set<String> keys = null;
		int numCleared = 0;
		if ("all".equalsIgnoreCase(key)) {
			keys = jedisAdaptor.keys("*");
		} else {
			keys = new HashSet<String>();
			String[] keyArray = key.split(",");
			for (String k : keyArray) {
				keys.addAll(jedisAdaptor.keys(k));
			}
		}
		numCleared += jedisAdaptor.delete(keys.toArray(new String[0]));
		return numCleared;

	}

	/**
	 * get all cache keys in pattern specified by key parameter.
	 * 
	 * @param key
	 * @return
	 */
	public static Set<String> getKeys(String key) {

		Set<String> keys = null;
		if (key == null || "all".equalsIgnoreCase(key)) {
			keys = jedisAdaptor.keys("*");
		} else {
			keys = new HashSet<String>();
			String[] keyArray = key.split(",");
			for (String k : keyArray) {
				keys.addAll(jedisAdaptor.keys(k));
			}
		}
		return keys;

	}

}
