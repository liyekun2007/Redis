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
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryList() {
        String key ="SHOP_TYPE";
        String shoptype = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shoptype)){
            List<ShopType> shopTypeList = JSONUtil.toList(shoptype, ShopType.class);
            return Result.ok(shopTypeList);
        }
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        if(shopTypes.isEmpty()){
            return Result.fail("未找到商铺信息");

        }
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shopTypes));
        return Result.ok(shopTypes);
    }
}
