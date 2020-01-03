package com.wuben.clustermanager.service;


import com.alibaba.fastjson.JSONObject;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.*;
import com.cloudera.api.v10.HostsResourceV10;
import com.cloudera.api.v11.TimeSeriesResourceV11;
import com.cloudera.api.v19.RootResourceV19;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

@Service
public class ClouderMangerService {

     static String url = "wbbigdata00";
     static int port = 7180;
     static String username = "admin";
     static String password = "admin";
     static RootResourceV19 apiRoot;

    static {
        apiRoot = new ClouderaManagerClientBuilder()
                .withHost(url)
                .withPort(port)
                .withUsernamePassword(username, password)
                .build()
                .getRootV19();
    }

    /**
     * 集群cpu
     */
     static String cpuQuery = "SELECT cpu_percent_across_hosts WHERE entityName = 1 AND category = CLUSTER";
    /**
     * 集群磁盘io
     */
     static String diskioQuery = "SELECT total_read_bytes_rate_across_disks+total_write_bytes_rate_across_disks WHERE entityName = 1 AND category = CLUSTER";
    /**
     * 集群网络io
     */
     static String networkQuery = "SELECT total_bytes_receive_rate_across_network_interfaces+total_bytes_transmit_rate_across_network_interfaces WHERE entityName = 1 AND category = CLUSTER";
    /**
     * 集群hdfs io
     */
     static String hdfsioQuery = "SELECT total_bytes_read_rate_across_datanodes+total_bytes_written_rate_across_datanodes WHERE entityName = hdfs AND category = SERVICE";

    /**
     * 集群重要警报
     */
     static String clusterAlertsRateQuery = "select integral(alerts_rate) where entityName=yarn";
    /**
     * 集群关键事件
     */
     static String clusterEventsCriticalRateQuery = "select integral(events_critical_rate) where entityName=yarn";
    /**
     * 集群重要事件
     */
     static String clusterEventsImportantRateQuery = "select integral(events_important_rate) where entityName=yarn";

    /**
     * 正在运行的应用程序
     */
     static String appsRunningCumulativeQuery = "SELECT apps_running_cumulative WHERE entityName = yarn:root AND category = YARN_POOL";

    /**
     * 入驻单位中所有在线用户
     */
     static String activeUserQuery = "SELECT hue_users_active WHERE entityName = hue-HUE_SERVER-ae57fd7a8981f1440d0aaae51abaa549 AND category = ROLE";


    /**
     * 在线用户
     */
    public List<JSONObject> getActiveUser() {
        return getSeriesDataList(activeUserQuery);
    }

    /**
     * 资源列队
     *
     * @return
     */
    public List<JSONObject> getAppsRunningCumulative() {
        return getSeriesDataList(appsRunningCumulativeQuery);
    }

    /**
     * 集群运行警报
     *
     * @return
     */
    public JSONObject getClusterEvent() {
        JSONObject result = new JSONObject();
        result.put("clusterAlertsRate", getSeriesDataList(clusterAlertsRateQuery));
        result.put("clusterEventsCriticalRate", getSeriesDataList(clusterEventsCriticalRateQuery));
        result.put("clusterEventsImportantRate", getSeriesDataList(clusterEventsImportantRateQuery));
        return result;
    }

    /**
     * 集群CPU使用情况
     *
     * @return
     */
    public JSONObject getClusterCPU() {
        JSONObject result = new JSONObject();
        result.put("cpu", getHostsUsedCpu());
        return result;
    }

    /**
     * 集群主机内存磁盘使用情况（集群管理工具会每一分钟取一次）
     *
     * @return
     */
    public JSONObject getClusterRes() {
        JSONObject result = new JSONObject();
        result.put("memory", getClusterHostsMemory());
        result.put("disk", getClusterHostsDisk());
        return result;
    }

    /**
     * 集群各I/O情况（集群管理工具会每一分钟取一次）
     *
     * @return
     */
    public JSONObject getClusterIo() {
        JSONObject result = new JSONObject();
        result.put("diskio", getSeriesData(diskioQuery));
        result.put("network", getSeriesData(networkQuery));
        result.put("hdfsio", getSeriesData(hdfsioQuery));
        return result;
    }


    /**
     * 查询一个区间数据
     *
     * @param query
     * @return
     */
    private List<JSONObject> getSeriesDataList(String query) {
        List<JSONObject> listObject = new ArrayList<JSONObject>();
        TimeSeriesResourceV11 v11 = apiRoot.getTimeSeriesResource();
        ApiTimeSeriesResponseList responsesResult = v11.queryTimeSeries(query, getTime(-15), "now");
        ApiTimeSeries timeSeries = responsesResult.getResponses().get(0).getTimeSeries().get(0);
        List<ApiTimeSeriesData> list = timeSeries.getData();
        if (list.size() > 0) {
            Double value = 0.00;
            for (int i = 0; i < list.size(); i++) {
                JSONObject dateObject = new JSONObject();
                ApiTimeSeriesData data = list.get(i);
                if (data != null) {
                    value = data.getValue();
                }
                dateObject.put("date", new SimpleDateFormat("yyyy-MM-dd HH:mm").format(data.getTimestamp()));
                dateObject.put("value", value);
                //获取单位
                ApiTimeSeriesMetadata metadata = timeSeries.getMetadata();
                dateObject.put("unit", metadata.getUnitNumerators().get(0));
                listObject.add(dateObject);
            }
        }
        return listObject;
    }

    /**
     * 简单单个数据查询
     *
     * @param query
     * @return
     */
    private JSONObject getSeriesData(String query) {
        JSONObject result = new JSONObject();
        TimeSeriesResourceV11 v11 = apiRoot.getTimeSeriesResource();
        //查询2分钟之前的数据，集群中每一分钟统计一次，总能查到数据必须是2分钟之前，去最后一条，因为可能存在当前还未统计则查不到数据
        ApiTimeSeriesResponseList responsesResult = v11.queryTimeSeries(query, getTime(-2), "now");
        ApiTimeSeries timeSeries = responsesResult.getResponses().get(0).getTimeSeries().get(0);
        List<ApiTimeSeriesData> list = timeSeries.getData();
        String value = null;
        if (list.size() > 0) {
            ApiTimeSeriesData data = list.get(0);
            if (data != null) {
                value = String.valueOf(data.getValue());
            }
        }
        //获取单位
        ApiTimeSeriesMetadata metadata = timeSeries.getMetadata();
        result.put("value", value);
        result.put("unit", metadata.getUnitNumerators().get(0));
        return result;
    }

    /**
     * 获取集群中所有主机内存使用量
     *
     * @return
     */
    private JSONObject getClusterHostsMemory() {
        JSONObject result = new JSONObject();
        HostsResourceV10 hostsResource = apiRoot.getHostsResource();
        List<ApiHost> hostList = hostsResource.readHosts(DataView.SUMMARY).getHosts();
        for (ApiHost apiHost : hostList) {
            JSONObject host = new JSONObject();
            String queryUsed = "SELECT physical_memory_used WHERE hostId= " + apiHost.getHostId() + "";
            host.put("used", getSeriesData(queryUsed));
            String queryTotal = "SELECT physical_memory_total WHERE hostId= " + apiHost.getHostId() + "";
            host.put("total", getSeriesData(queryTotal));
            result.put(apiHost.getHostname(), host);
        }
        return result;
    }

    /**
     * 获取集群所有主机磁盘使用情况
     *
     * @return
     */
    private JSONObject getClusterHostsDisk() {
        JSONObject result = new JSONObject();
        HostsResourceV10 hostsResource = apiRoot.getHostsResource();
        List<ApiHost> hostList = hostsResource.readHosts(DataView.SUMMARY).getHosts();
        for (ApiHost apiHost : hostList) {
            JSONObject host = new JSONObject();
            String queryUsed = "SELECT total_capacity_used_across_directories  WHERE hostId= " + apiHost.getHostId() + "";
            host.put("used", getDiskList(queryUsed));
            String queryTotal = "SELECT total_capacity_across_directories WHERE hostId= " + apiHost.getHostId() + "";
            host.put("total", getDiskList(queryTotal));
            result.put(apiHost.getHostname(), host);
        }
        return result;
    }

    /**
     * 获取集群所有主机cpu使用情况
     *
     * @return
     */
    private JSONObject getHostsUsedCpu() {
        JSONObject result = new JSONObject();
        HostsResourceV10 hostsResource = apiRoot.getHostsResource();
        List<ApiHost> hostList = hostsResource.readHosts(DataView.SUMMARY).getHosts();
        for (ApiHost apiHost : hostList) {
            JSONObject host = new JSONObject();
            String queryUsedCpu = "SELECT cpu_percent WHERE hostId= " + apiHost.getHostId() + " AND category = HOST ";
            host.put("used", getDiskList(queryUsedCpu));
            result.put(apiHost.getHostname(), host);
        }
        return result;
    }


    private JSONObject getDiskList(String query) {
        JSONObject result = new JSONObject();
        TimeSeriesResourceV11 v11 = apiRoot.getTimeSeriesResource();
        //查询2分钟之前的数据，集群中每一分钟统计一次，总能查到数据必须是2分钟之前，去最后一条，因为可能存在当前还未统计则查不到数据
        ApiTimeSeriesResponseList responsesResult = v11.queryTimeSeries(query, getTime(-2), "now");
        List<ApiTimeSeries> timeSeriesList = responsesResult.getResponses().get(0).getTimeSeries();
        Double value = 0.00;
        String unit = "";
        if (timeSeriesList.size() > 0) {
            for (int i = 0; i < timeSeriesList.size(); i++) {
                ApiTimeSeries timeSeries = timeSeriesList.get(i);
                List<ApiTimeSeriesData> list = timeSeries.getData();
                if (list.size() > 0) {
                    ApiTimeSeriesData data = list.get(list.size() - 1);
                    if (data != null) {
                        value = +data.getValue();
                    }
                }
                ApiTimeSeriesMetadata metadata = timeSeries.getMetadata();
                unit = metadata.getUnitNumerators().get(0);
            }
            result.put("value", String.valueOf(value));
            result.put("unit", unit);
        }
        return result;

    }

    private String getTime(int minute) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, minute);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd&HH:mm");
        return format.format(cal.getTime()).replace("&", "T");
    }
}

