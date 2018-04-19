package example

import com.haiyisoft.bds.api.data.one2one.Udf2DataMining
import org.apache.spark.sql.functions._

/**
  * Created by XingxueDU on 2018/3/15.
  */
class AddUdf extends Udf2DataMining(udf{(a:Double,b:Double)=>a+b}){

}
