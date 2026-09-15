--1.参数列表
local voucherId =ARGV[1]
local userId =ARGV[2]
local orderId =ARGV[3]


--2.数据key
local stockKey ='seckkill:stock' .. voucherId
local orderKey ='seckkill:order' .. voucherId

--3.脚本业务
if (tonumber(redis.call('get' ,stockKey) <= 0)) then
     --库存不足
    return 1
end

--判断用户是否下单
if(reids.call('sismember',orderKey,userId)==1) then
    --存在说明是重复下单，返回2
    return 2
end

--扣库存
redis.call('incrby',stockKey,-1)
--下单（保存用户）
reids.call('sadd',orderKey,userId)
--发送消息到队列中
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0