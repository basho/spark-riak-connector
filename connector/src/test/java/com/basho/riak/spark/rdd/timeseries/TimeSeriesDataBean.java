/*******************************************************************************
 * Copyright (c) 2016 IBM Corp.
 * 
 * Created by Basho Technologies for IBM
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *******************************************************************************/
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