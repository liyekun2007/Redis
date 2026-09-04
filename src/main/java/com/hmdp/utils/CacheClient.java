package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

@Component
@Slf4j
public class CacheClient {

    private StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public void set(String key , Object value , Long time , TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpire(String key , Object value , Long time , TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type , Function<ID,R> dbFallBack,Long time , TimeUnit unit){

        String key =keyPrefix+id;
        //从redis查询商户id
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        //判断命中是否为空值
        if(json !=null){
            return null;
        }
        //不存在，根据id查询
        R r = dbFallBack.apply(id);
        if(r ==null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //存在，写入redis
        this.set(key,r,time,unit);
        //返回
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix,  ID id,Class<R> type,Function<ID,R> dbFallBack,Long time , TimeUnit unit){

        String key =keyPrefix+id;
        //从redis查询商户id
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isBlank(json)){
            return null;
        }

        //4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1.没有过期，直接返回店铺对象
            return r;
        }

        //5.2.过期了，则需要重建缓存
        //6.缓存重建
        //6.1.获取互斥锁
        String lockKey =LOCK_SHOP_KEY+ id;
        boolean isLock = tryLock(lockKey);
        //6.2.判断是否获取锁成功
        if(isLock){
            //6.3，获取成功，开启独立线程
            CACHE_REBUILD_EXECUTOR.submit(()->{
                //重建缓存
                try {
                    R r1 = dbFallBack.apply(id);
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unLock(lockKey);
                }

            });

        }

        //6.4，获取失败，返回过期店铺信息

        //返回
        return r;
    }

    //获取锁
    private boolean tryLock(String key ){

        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private  void unLock(String key){
        stringRedisTemplate.delete(key);
    }



}
