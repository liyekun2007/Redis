package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private  StringRedisTemplate stringRedisTemplate;
    @Override
    public Result show() {
        String key ="SHOP_TYPE";
        //从redis中查找
        String shoptype = stringRedisTemplate.opsForValue().get(key);
        //如果不为空，直接返回缓存中的信息
        if(StrUtil.isNotBlank(shoptype)){
            List<ShopType> shopTypeList = JSONUtil.toList(shoptype, ShopType.class);
            return Result.ok(shopTypeList);
        }
        //如果为空，就从数据库中查找
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        //如或数据库中为空，就直接返回
        if(shopTypeList.isEmpty()){
            return Result.fail("没有从数据库中查到信息");

        }
        //将数据缓存存到redis当中

        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shopTypeList));
        return Result.ok(shopTypeList);
    }
}
