package com.haiyisoft.bds.javaapi.action;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by XingxueDU on 2018/3/15.
 */
public interface Model extends Action{
    public void saveTo(String path);
    public Model loadFrom(String path);
    public void setLoadPath(String path);
    public Dataset<Row> transform(Dataset<Row> data);
}
