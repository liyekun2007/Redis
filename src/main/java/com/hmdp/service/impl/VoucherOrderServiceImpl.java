package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀还没开始");
//        }
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//        if (voucher.getStock()<1) {
//            return Result.fail("库存不足");
//
//        }
//        //一人一单
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock( );
//        //判断是否获取锁成功
//        if(!isLock){
//            return Result.fail("不允许重复下单");
//        }
//        //获取代理对象
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//
//    }

    private static  final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        // 创建 Stream 和消费者组（如果不存在）
        try {
            stringRedisTemplate.opsForStream().createGroup("stream.orders", ReadOffset.from("0"), "g1");
        } catch (Exception e) {
            // 消费者组已存在，忽略
        }
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private  class VoucherOrderHandler implements Runnable{
        String queueName ="stream.orders";
        @Override
        public void run() {
            while (true){

                try {
                    //1.获取订单中的消息队列信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息获取是否成功
                    if(list ==null || list.isEmpty()){
                        //3.失败，说明没有消息继续下次循环
                        continue;
                    }
                    //解析订单中的信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //成功，则创建订单
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }

            }
        }

        private void handlePendingList() {
            while (true){

                try {
                    //1.获取订单中的pending-list队列信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息获取是否成功
                    if(list ==null || list.isEmpty()){
                        //3.失败，说明没有消息继续下次循环
                        break;
                    }
                    //解析订单中的信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //成功，则创建订单
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }

            }
        }
//    private BlockingQueue<VoucherOrder> orderTasks =new ArrayBlockingQueue<>(1024*1024);
//    private  class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while (true){
//
//                try {
//                    //1.获取订单中的队列信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //2.创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                }
//
//            }
//        }

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId =voucherOrder.getUserId();
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            //获取锁
            boolean isLock = lock.tryLock( );
            //判断是否获取锁成功
            if(!isLock){
                log.error("不允许重复下单");
                return;
            }
            //获取代理对象
            try {
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                lock.unlock();
            }
        }
    }
    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //判断结果是否为0
        int r =result.intValue();
        if (r!=0){
            return Result.fail(r==1 ? "库存不足":"不能重复下单");
        }

        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //返回订单id

        return Result.ok(orderId);
    }
    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        //执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        //判断结果是否为0
//        int r =result.intValue();
//        if (r!=0){
//            return Result.fail(r==1 ? "库存不足":"不能重复下单");
//        }
//        //为0，有购买资格，把下单信息保存到阻塞队列中
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//
//        orderTasks.add(voucherOrder);
//
//        //获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        //返回订单id
//
//        return Result.ok(orderId);
//    }


    @Transactional
    public  void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次");
            return;
        }
        boolean success = seckillVoucherService.update()
                .setSql("stock =stock-1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }

        save(voucherOrder);
    }
}
