package com.bcone.logistics.util

object PathProcessor {
  def generateFileNameAndFileConfigName(path:String):List[String] = {
    val inputFileConfigName=path
      .split("/")
      .toList
      .tail
      .tail
      .tail
      .reverse
      .tail
      .reverse.mkString("_")
    if(inputFileConfigName.split("_").length==4) {
      val inputFileName=path.split("/").toList.reverse.head
      val inputFileFormat=inputFileName.split('.')(1)
      List(inputFileConfigName,inputFileName,inputFileFormat)
    }
    else {
      List.empty[String]
    }

  }

}
