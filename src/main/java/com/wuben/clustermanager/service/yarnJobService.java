package com.wuben.clustermanager.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class yarnJobService {
    public static EnumSet<YarnApplicationState> appStates = null;
    public static EnumSet<YarnApplicationState> finishedStates = null;
    public static List<ApplicationReport> appsReport = null;
    public static YarnClient client = null;

    static {
        client = YarnClient.createYarnClient();
        Configuration conf = new Configuration();
        client.init(conf);
        client.start();
    }

    public JSONObject getRunningApp() {
        appStates = EnumSet.noneOf(YarnApplicationState.class);
        if (appStates.isEmpty()) {
            appStates.add(YarnApplicationState.RUNNING);
            appStates.add(YarnApplicationState.FINISHED);
        }
        try {
            //返回EnumSet<YarnApplicationState>中个人任务是running状态的任务
            appsReport = client.getApplications(appStates);
            for (ApplicationReport appReport : appsReport) {
                Map<String, Object> map = new HashMap<>();
                map.put("jobID", appReport.getApplicationId().toString());
                map.put("jobName", appReport.getName());
                map.put("jobType", appReport.getApplicationType());
                map.put("jobStatus", appReport.getFinalApplicationStatus());
                map.put("clusterDetails", appReport.getApplicationResourceUsageReport().toString());
                map.put("progress", appReport.getProgress());
                return new JSONObject(map);
            }
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
