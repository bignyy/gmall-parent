package com.atguigu.gmall0213.publisher.service;

import java.util.Map;

public interface DauService {
    //求某日日活总值
    public Long  getDauTotal(String date);

    //求某日日活的分时值
    public Map getDauHourCount(String date);
}
