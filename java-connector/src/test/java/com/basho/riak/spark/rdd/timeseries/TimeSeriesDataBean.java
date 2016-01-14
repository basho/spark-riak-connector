package com.basho.riak.spark.rdd.timeseries;

@SuppressWarnings("unused")
public class TimeSeriesDataBean {

    private long time;
    private String user_id;
    private double temperature_k;

    public TimeSeriesDataBean() {
    }

    public TimeSeriesDataBean(long time, String user_id, double temperature_k) {
        this.time = time;
        this.user_id = user_id;
        this.temperature_k = temperature_k;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public double getTemperature_k() {
        return temperature_k;
    }

    public void setTemperature_k(double temperature_k) {
        this.temperature_k = temperature_k;
    }
}