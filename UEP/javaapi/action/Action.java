package com.haiyisoft.bds.javaapi.action;

/**
 * Created by XingxueDU on 2018/3/15.
 */
public interface Action {
    public void run(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> data);
    public String[] needCol();
    /**
     * 结果的输出方式？
     */
    public void setSavePath(String path);//可能需要别的格式
}
