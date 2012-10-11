package com.aug3.storage.redisclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.thoughtworks.xstream.XStream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class JedisAdaptor {
    private static final XStream xmlXStream = new XStream();
    
	private Jedis redis = JedisPoolMgr.getJedisPool().getResource();

	public boolean exists(String key) {
		return redis.exists(key);
	}

	public String get(String key) {
		return redis.get(key);
	}

	public String set(String key, String value) {
		return redis.set(key, value);
	}

	public String mset(String... keysvalues) {
		return redis.mset(keysvalues);
	}

	public List<String> mget(String... keys) {
		return redis.mget(keys);
	}

	public Long expire(String key, int seconds) {
		return redis.expire(key, seconds);
	}

	public Long expireAt(String key, long unixTime) {
		return redis.expireAt(key, unixTime);
	}

	public void set(String key, String value, int seconds) {
		set(key, value);
		expire(key, seconds);
	}

	public Long remove(String... keys) {
		return redis.del(keys);
	}

	public String hmset(String key, Map<String, String> hash) {
		return redis.hmset(key, hash);
	}

	public Map<String, String> hgetAll(String key) {
		return redis.hgetAll(key);
	}

	public Long incr(String key) {
		return redis.incr(key);
	}

	public Jedis getRedis() {
		return redis;
	}
	
	
    /**
     * 
      * @Title: getFromXml
      * @Description: get xml-formatted value through the given key and convert it to Object and 
      *               then you can get a T instance through class downcast. The return object could
      *               be converted to Object and Collection.
      *               
      * @param key
      * @return Object
     * @throws JedisException 
     */
    public Object getFromXml(String key) throws JedisException {
        return xmlXStream.fromXML(redis.get(key));
    }
	
	 /**
     * 
      * @Title: getFormXML
      * @Description: 
      * @param keys
      * @return List
     * @throws JedisException 
     */
    public List<Object> getFormXML(String[] keys) throws JedisException {
        List<String> arrs = redis.mget(keys);
        List<Object> result = new ArrayList<Object>(arrs.size());
        for (String arr : arrs) {
            result.add(xmlXStream.fromXML(arr));
        }
        return result;
    }
    
    /**
     * @throws JedisException 
     * 
      * @Title: putAsXML
      * @Description: put the object(-->xml) in redis
      * @param key
      * @param value
      * @param expiredSeconds --int(seconds)
      * @return  OK if sccessful,null if exception
      * @throws
     */
    public String setAsXML(String key, Object value, int expiredSeconds) throws JedisException{
            return redis.setex(key, expiredSeconds, xmlXStream.toXML(value));
    }
}
