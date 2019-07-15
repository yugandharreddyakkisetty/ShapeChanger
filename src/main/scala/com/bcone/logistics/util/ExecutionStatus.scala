package com.bcone.logistics.util

object ExecutionStatus {
  val invalidPath:Int=1001
  val extraColumns:Int=1002
  val missingColumn:Int=1003
  val blankColumns:Int=1004
  val dictionaryNotDefined:Int=1005
  val emptyFile:Int=1006
  val success:Int=0
  val codes:Map[Int,String]=Map(
    0->"Validation Success",
    1001->"Invalid Path",
    1002->"Extra column(s) found",
    1003->"Missing column(s)",
    1004->"Blanks columns found",
    1005->"Dictionary Not Defined",
    1006->"Empty File")

}
