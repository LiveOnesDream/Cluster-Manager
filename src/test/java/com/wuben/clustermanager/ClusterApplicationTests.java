package com.wuben.clustermanager;

import com.alibaba.fastjson.JSONObject;
import com.wuben.clustermanager.service.ClouderMangerService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ClusterApplicationTests {

    @Autowired
    ClouderMangerService service;

    @Test
    void contextLoads() {
        JSONObject clusterEvent = service.getClusterEvent();
        System.out.println(clusterEvent);
    }


}
