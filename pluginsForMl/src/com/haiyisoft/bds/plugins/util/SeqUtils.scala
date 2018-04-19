package com.haiyisoft.bds.plugins.util

import java.io.{OutputStream, InputStreamReader, BufferedReader, InputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.reflect.ClassTag
import java.{util=>ju}

import scala.util.Try

/**
  * Created by DU Xingxue on 2017/6/27.
  *
  * @author DU Xingxue
  * @version 8.2.8
  */
object SeqUtils {

  def Array2ArrayBuffer[T:ClassTag](t:Array[T])={
    val res = mutable.ArrayBuffer[T]()
    for(x<-t)res+=x
    res
  }
  def List2Array[T:ClassTag](l:List[T])={
    l.toArray
  }
  
  
  def SeqSortByHash[T:ClassTag](array:Seq[T])={
    array.sortBy(x=>if(x==null)0 else x.toString.hashCode)
  }

  def StringSortByHash(str:String,regex:String)={
    SeqSortByHash(str.split(regex)).mkString(regex)
  }
  def List2Array[T:ClassTag](list:java.util.List[T]):Array[T]={
    if(list.isInstanceOf[ju.ArrayList[T]]){
      val size = list.size()
      val t = (0 until size).par.map(x=>list.get(x))
      t.toArray
    }else{
      val it = list.iterator()
      val buffer = new mutable.ArrayBuffer[T](list.size)
      while(it.hasNext)buffer+=it.next()
      buffer.toArray
    }
  }

  /**
    * 将数组标准化到[minVal,maxVal]之间
    */
  def minMaxArray(array:Seq[Double],minVal:Double=0d,maxVal:Double=1d):Seq[Double]={
    var min = Double.MaxValue
    var max = Double.MinValue
    array.foreach{k=>
      if(min>k)min=k
      if(max<k)max=k
    }
    val diff = max - min
    for(x<-array)yield{
      (maxVal-minVal)*(x-min)/diff-minVal
    }
  }

  /**
    * 将数组缩放到和为sumVal
    */
  def sumArray(array:Seq[Double],sumVal:Double=1d):Seq[Double]={
    val oldSum = array.sum
    val multiple = sumVal/oldSum
    for(x<-array)yield x*multiple
  }
  /**
    * 最多相同数据T
    *
    * @param array 给定表
    * @param LeftToRight 获取第一个/最后一个T
    * @return 个数最多的T
    */
  def maxCount[T:ClassTag](array:Seq[T],LeftToRight:Boolean = true):Option[T]={
    val arr = array.toArray
    val map = scala.collection.mutable.Map[T,Int]()
    arr.foreach(x=>if(map.contains(x))map(x)+=1 else map(x)=1)
    val max = map.values.max
    if(max==1) None
    else{
      val s = (for(att <- map if att._2==max)yield att._1).toArray
      if(LeftToRight)Some(s(0))
      else Some(s(s.length-1))
    }
  }
  def concurrentHashMap[K,V]()={
    import scala.collection.convert.decorateAsScala._
    val map: scala.collection.concurrent.Map[K, V] = new java.util.concurrent.ConcurrentHashMap[K,V]().asScala
    map
  }


  def removeElementByIndex[T](array:Seq[T], index:Int):Seq[T]={
    for(x<-array.indices if x!=index)yield array(x)
  }
  def removeElement[T](array:Seq[T],elem:T):Seq[T]={
    for(x<-array if x!=elem)yield x
  }
  /**
    * 最小值所在的index
    */
  def minIndex[B<:AnyVal](array:Seq[B])(implicit ord: Ordering[B])={
    if(array.isEmpty)throw new IndexOutOfBoundsException("input array shouldn't be empty")
    var minIndex = 0
    var minValue = array.head
    for(x<-array.indices if ord.compare(array(x),minValue)<0){
      minIndex = x
      minValue = array(x)
    }
    (minIndex,minValue)
  }
  /**
    * 最大值所在的index
    */
  def maxIndex[B<:AnyVal](array:Seq[B])(implicit ord: Ordering[B])={
    if(array.isEmpty)throw new IndexOutOfBoundsException("input array shouldn't be empty")
    var maxIndex = 0
    var maxValue = array.head
    for(x<-array.indices if ord.compare(array(x),maxValue)>0){
      maxIndex = x
      maxValue = array(x)
    }
    (maxIndex,maxValue)
  }

  /**
    * 最小正值所在的index位置。如果不存在正值则返回-1
    */
  def positiveMinIndex[B<:AnyVal](array:Array[B])(implicit toDouble:B=>Double): Int ={
    var b:B = array.head
    var idx = if(b<0)-1 else 0
    for(idxx<-1 until array.length){
      val value = array(idxx)
      if(value>0){
        if(idx<0||b>value){
          b = value
          idx = idxx
        }
      }
    }
    idx
  }

  /**
    * Array的切分方式，不支持正则
 *
    val a = Array[Int](1,2,1,2,1,1)
    val b = split(a,1,limit = 2,deleteBlank = true)
    println(b.map(_.mkString("[",",","]")).mkString(","))
    ----------------
    >>>[2],[2],[1]
    *
    * @param in 待切分表
    * @param regex 分隔符
    * @param from 起始指针
    * @param limit 拆分次数，该值为负数时不拆分。注意：一个正数的限制值可能导致最后一个元素中出现分隔符
    * @param deleteBlank 是否删除拆分出的空元素。
    */
  def split[T:ClassTag](in:Array[T], regex:T, from:Int=0, limit:Int= -1, deleteBlank:Boolean = false):Array[Array[T]]={
    var rest = if(limit<0) -2 else limit+1
    var idx = from
    var res:Array[Array[T]] = Array[Array[T]]()
    if(in.nonEmpty) while(rest!=0&&idx<=in.length){
      val nextIdx = in.indexOf(regex,idx)
      val sub =if(nextIdx<0||rest ==1) {
        rest=0
        for(x<-idx until in.length)yield in(x)
      }
      else{
        rest -= 1
        for(x<-idx until nextIdx)yield in(x)
      }
      idx = nextIdx+1
      if(!deleteBlank||sub.nonEmpty)res :+= sub.toArray
      else rest += 1
    }
    res
  }
  /**
    * 将Seq根据长度进行分割,目的是自动换行或者使得每个集合存在的元素小于给定长度
    *
    * @param input 输入的字符串
    * @param length 给定的分割长度
    * @return 存有固定长度的字符串组的集合
    */
  def splitArrayByLength[T:ClassTag](input:Seq[T],length:Int): Seq[Seq[T]] ={
    val res = scala.collection.mutable.ArrayBuffer[Seq[T]]()
    val remainder =  input.length % length
    val number = (BigDecimal(input.length)/length).setScale(0,BigDecimal.RoundingMode.DOWN).toInt
    for(index <- 0 until number){
      val beginIndex = index*length
      val endIndex = (index+1)*length
      val s = new Array[T](length)
      for(x<-beginIndex until endIndex)s(x-beginIndex)=input(x)
      res+=s
    }
    if(remainder>0){
      val beginIndex = number * length
      val endIndex = input.length
      val s = new Array[T](remainder)
      for(x<-beginIndex until endIndex)s(x-beginIndex)=input(x)
      res+=s
    }
    res
  }
/*
  class Matrix[T:ClassTag](length_row:Int,length_col:Int) extends scala.collection.AbstractSeq[scala.collection.AbstractSeq[T]] {
    val array = new Array[Array[T]](length_row)
    val length = length_row
    def apply(idx:Int)=array(idx)
    def apply(row:Int,col:Int) = array(row)(col)
  }
  */
}
