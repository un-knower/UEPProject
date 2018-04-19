//package com.haiyisoft.bds.plugins.loader
//
//import com.haiyisoft.bds.api.data.DataLoadInter
//import com.haiyisoft.bds.api.param.ParamFromUser
//import org.apache.spark.sql.DataFrame
//
///**
//  * Created by XingxueDU on 2018/3/23.
//  */
//class FlowDataLoader extends DataLoadInter{
//  override def getData: DataFrame = {
//    throw new NoSuchMethodException()
//  }
//
//  /**
//    * 获取该类必须设置的参数列表
//    *
//    * @return 参数列表
//    */
//  override def getNecessaryParamList: Array[ParamFromUser] = {
//    val IS_FLOW_DATALOADER_SPECIAL_PARAM = ParamFromUser.String("flow","").setTypeName("subflow")
//    Array(IS_FLOW_DATALOADER_SPECIAL_PARAM)
//  }
//
//  override def getPluginName: String = "加载既有流程"
//}
