package com.haiyisoft.bds.plugins.process

import com.haiyisoft.bds.api.data.DataJoin
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.DataFrame

/**
  * Created by Namhwik on 2018/3/20.
  */
class JoinProcessor extends DataJoin {

  override def join(fstTable: DataFrame, sndTable: DataFrame): DataFrame = {
    val fst = getFirstDfKeyCol
    val snd = getSecondDfKeyCol
    val joinType = getJoinType
    if (fst != snd) fstTable.join(sndTable, fstTable(fst) === sndTable(snd), getJoinType)
    else if (joinType == "inner") fstTable.join(sndTable, fst)
    else {
      fstTable
        .withColumnRenamed(fst, fst + "fst_n")
        .join(sndTable, fstTable(fst + "fst_n") === sndTable(snd), getJoinType)
        .drop(fst + "fst_n")
    }
  }

  final val LEFT_KEY_COL_NAME_KEY = "join.keycol.name.first"
  final val RIGHT_KEY_COL_NAME_KEY = "join.keycol.name.second"
  final val JOIN_TYPE_KEY = "join.type"
  final val LEFT_TABLE = "join.left"
  final val RIGHT_TABLE = "join.right"

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val fst = ParamFromUser.String(LEFT_KEY_COL_NAME_KEY, "#请输入来自第一个表的关键列列名", notEmpty = true)
      .setShortName("左表聚合列")
      .setNeedSchema(true)
    val snd = ParamFromUser.String(RIGHT_KEY_COL_NAME_KEY, "#请输入来自第二个表的关键列列名", notEmpty = true)
      .setShortName("右表聚合列")
      .setNeedSchema(true)
    val leftTable = ParamFromUser.String(LEFT_TABLE, "#请输入join的左表", notEmpty = true)
      .setTypeName("joinLeft")
      .setShortName("选择左表")
    val rightTable = ParamFromUser.String(RIGHT_TABLE, "#请输入join的右表", notEmpty = true)
      .setTypeName("joinRight")
      .setShortName("选择右表")
    val joinType = ParamFromUser
      .SelectBox(JOIN_TYPE_KEY, "请选择连接方式#默认inner", "inner", "outer", "left_outer", "right_outer", "leftsemi")
      .setShortName("连接操作")
    Array(fst, snd, leftTable, rightTable, joinType)
  }

  def getJoinType: String = getStringParam(JOIN_TYPE_KEY)

  def getFirstDfKeyCol: String = getStringParam(LEFT_KEY_COL_NAME_KEY)

  def getSecondDfKeyCol: String = getStringParam(RIGHT_KEY_COL_NAME_KEY)

  override def getPluginName: String = "Join模块"


}
