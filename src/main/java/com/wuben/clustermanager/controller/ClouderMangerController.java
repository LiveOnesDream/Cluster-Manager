package com.wuben.clustermanager.controller;

import com.alibaba.fastjson.JSONObject;
import com.wuben.clustermanager.service.ClouderMangerService;
import com.wuben.clustermanager.service.yarnJobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClouderMangerController {
    @Autowired
    ClouderMangerService service;

    @Autowired
    yarnJobService yarnService;

    @RequestMapping("/ClusterEvent")
    JSONObject getClusterEvent() {
        return service.getClusterEvent();
    }

    @RequestMapping("/ClusterCPU")
    JSONObject getClusterCPU() {
        return service.getClusterCPU();
    }

    @RequestMapping("/ClusterRes")
    JSONObject getClusterRes() {
        return service.getClusterRes();
    }

    @RequestMapping("/ClusterIo")
    JSONObject getClusterIo() {
        return service.getClusterIo();
    }

    @RequestMapping("/yarn")
    JSONObject getYarnJob() {
        return yarnService.getRunningApp();
    }
}
