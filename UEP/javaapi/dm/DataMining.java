package com.haiyisoft.bds.javaapi.dm;

/**用于数据处理部分
 * 例如对UDF进行包装
 * Created by XingxueDU on 2018/3/15.
 */
public interface DataMining {
    public org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> transform(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> data);
    public DataMining setInputCols(String... name);
    public DataMining setOutputCol(String name);

    public String[] getInputCols();
    public String getOutputCol();
}
