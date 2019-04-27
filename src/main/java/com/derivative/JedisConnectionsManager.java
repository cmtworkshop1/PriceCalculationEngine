package com.derivative;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisConnectionsManager {

    private static JedisPool jedisPool = null;

    public static void init(String url) {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxWaitMillis(60000);
        config.setMaxTotal(128);
        jedisPool =new JedisPool(config,url,6379,30000);

    }


    public static Jedis getJedis(int index){

        Jedis jedis  = jedisPool.getResource();
        if(!jedis.isConnected()){
            jedis.connect();
        }

        jedis.select(index);

        return jedis;
    }




}
