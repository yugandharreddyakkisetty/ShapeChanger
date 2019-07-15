package com.bcone.logistics.util


object ExceptionUtil {

  def throwCustomException(msg: String) : Unit = {
    throw new RuntimeException(msg)
  }

}
