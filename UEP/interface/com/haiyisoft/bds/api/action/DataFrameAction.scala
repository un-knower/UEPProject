package com.haiyisoft.bds.api.action

import com.haiyisoft.bds.api.param.save.HasSaveDataFrameParam
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
trait DataFrameAction extends Action[DataFrame] with HasSaveDataFrameParam
