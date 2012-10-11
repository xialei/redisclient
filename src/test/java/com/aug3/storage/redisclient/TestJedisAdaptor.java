package com.aug3.storage.redisclient;

import java.util.HashMap;
import java.util.List;

import org.junit.Test;

public class TestJedisAdaptor {

	@Test
	public void test() {
		JedisAdaptor jedis = new JedisAdaptor();

		String json = "{\"a\":1, \"b\" : {\"_id\":12, \"name\" : \"roger\"}}";
		jedis.set("test_by_roger_set", json);
		
		System.out.println(jedis.get("test_by_roger_set"));
		

		jedis.mset("test_by_roger_mset", "xia_1", "roger_2", "xia_2");
		List<String> names = jedis.mget("test_by_roger_mset", "roger_2");
		for (String name : names) {
			System.out.println(name);
		}

		HashMap<String, String> m = new HashMap<String, String>();
		m.put("name", "roger");
		m.put("age", "30");
		m.put("sex", "male");
		jedis.hmset("test_by_roger_hmset", m);
		
		jedis.remove("test_by_roger_set", "test_by_roger_mset", "test_by_roger_hmset");
	}

}
