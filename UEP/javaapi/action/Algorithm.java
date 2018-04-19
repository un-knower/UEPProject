package com.haiyisoft.bds.javaapi.action;

/**
 * Created by XingxueDU on 2018/3/15.
 */
public interface Algorithm extends Action{
    public Model fit(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> data);
}
