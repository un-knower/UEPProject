package com.haiyisoft.bds.javaapi.data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by XingxueDU on 2018/3/15.
 */
public interface DataGenerator {
    public Dataset<Row> getData();
}
