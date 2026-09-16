package com.hmdp;

import com.hmdp.entity.Blog;
import com.hmdp.service.IBlogService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private IBlogService blogService;

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
}
