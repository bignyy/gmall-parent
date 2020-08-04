package com.atguigu.gmall0213.publisher.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderService {

    public BigDecimal getOrderTotalAmount(String dt);
    public Map getOrderHourAmount(String dt);

}
