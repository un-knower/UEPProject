package com.haiyisoft.bds.api.param.save

/**
  * Created by XingxueDU on 2018/3/16.
  */
trait HasNotNeedToSaveParam[T] extends HasSaveParamFor[T]{

  override def save(stored:T):Unit={
    //Nothing to do
  }

}
