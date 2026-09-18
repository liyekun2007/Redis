package com.hmdp;

import com.hmdp.entity.Blog;
import com.hmdp.entity.Shop;
import com.hmdp.service.IBlogService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private IBlogService blogService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void testSaveShop(){
        shopService.saveShop2Redis(1L,10L);
    }

    @Test
    void testSaveBlogCache(){
        // 1. 查询所有博客
        List<Blog> blogs = blogService.list();
        // 2. 逐个写入 Redis（逻辑过期 30 分钟）
        for (Blog blog : blogs) {
            String key = RedisConstants.CACHE_BLOG_KEY + blog.getId();
            cacheClient.setWithLogicalExpire(key, blog, 30L, TimeUnit.MINUTES);
            System.out.println("缓存博客 id=" + blog.getId());
        }
        System.out.println("博客缓存预热完成，共 " + blogs.size() + " 条");
    }
    @Test
    void loadShopData(){
        //1.查询店铺信息
        List<Shop> list = shopService.list();
        //2.将店铺根据类型分组
        Map<Long,List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop ::getTypeId));
        //3.分批完成写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //3.1获取类型id
            Long typeId = entry.getKey();
            String key ="shop:geo:"+typeId;
            //3.2获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations =new ArrayList<>();
            //3.3写入redis
            for (Shop shop : value) {
                //stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(), shop.getY()),shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),new Point(shop.getX(),shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add(key,locations);

        }
    }
}
