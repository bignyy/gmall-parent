package com.atguigu.gmall0213.publisher.service.impl;

import com.atguigu.gmall0213.publisher.bean.HourAmount;
import com.atguigu.gmall0213.publisher.mapper.OrderWideMapper;
import com.atguigu.gmall0213.publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderServiceImpl implements OrderService {
    @Autowired
    OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderTotalAmount(String dt) {
        return orderWideMapper.getOrderTotalAmount(dt);
    }

    @Override
    public Map getOrderHourAmount(String dt) {
        List<HourAmount> hourAmountList = orderWideMapper.getOrderHourAmount(dt);
        // [{"hr:"11","orderAmount":202120.00},{"hr:"12","orderAmount":2323.00}.....]
        //  { "11":202120.00,"12":2323.00 .....}
        Map hourAmountMap=new HashMap();
        for (HourAmount hourAmount : hourAmountList) {
            hourAmountMap.put(hourAmount.getHr(),hourAmount.getOrderAmount());
        }
        return hourAmountMap;
    }
}
