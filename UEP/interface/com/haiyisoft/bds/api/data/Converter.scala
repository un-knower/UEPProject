package com.haiyisoft.bds.api.data

/**
  * Created by Namhwik on 2018/4/16.
  */
object Converter extends Enumeration {
  type TransType = Value
  val SHUFFLE,TRANSFORM,SELECT,FREE_COMBINATION: Converter.Value = Value
}
