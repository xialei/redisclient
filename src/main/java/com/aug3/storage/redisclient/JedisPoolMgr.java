package com.aug3.storage.redisclient;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolMgr {

	private static JedisPool pool;

	static {

		JedisPoolConfig config = new JedisPoolConfig();

		RedisConfig redisConfig = new RedisConfig();

		config.setMaxActive(redisConfig.getIntProperty(RedisConfig.REDIS_POOL_MAX_ACTIVE, 20));
		config.setMaxIdle(redisConfig.getIntProperty(RedisConfig.REDIS_POOL_MAX_IDLE, 10));
		config.setMaxWait(redisConfig.getIntProperty(RedisConfig.REDIS_POOL_MAX_WAIT, 1000));
		config.setTestOnBorrow(true);

		pool = new JedisPool(config, redisConfig.getProperty(RedisConfig.REDIS_HOST), redisConfig.getIntProperty(
				RedisConfig.REDIS_PORT, 6379));

	}

	public static JedisPool getJedisPool() {
		return pool;
	}

}
