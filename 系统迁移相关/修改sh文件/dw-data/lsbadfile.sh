#! /bin/bash

. ~/.profile


if [ $# != 1 ]; then
  echo Script Usage: ./lsbadfile.sh yyyymmdd
else
  rootfolder=/oracle/dw-data/
  pwdfolder=`pwd`
  datefolder=$1

  cd $rootfolder
  
  echo
  echo "******** 数据仓库每日数据处理结果统计 @ $datefolder： ********"
  echo
  echo "处理如下来源的数据："
  count=0
  for foldername in `ls -F | grep /$`
  do
    if [ -d "$rootfolder""$foldername""$datefolder" ];then

      filecnt=`ls -l "$rootfolder""$foldername""$datefolder"/bad | grep "^-" | wc -l`
      if [ $filecnt -ne 0 ]; then
            echo
	    echo "错误  @   $foldername"
	    echo "*** 发现错误文件！ 文件位置：""$rootfolder""$foldername""$datefolder"/bad
	    echo "错误数据文件列表如下："
        ls -l "$rootfolder""$foldername""$datefolder"/bad | grep  "^-"
      else
        echo
        echo "正常  @   $foldername"
      fi
      count=`expr $count + $filecnt`
    fi
  done
  echo
  if [ $count -ne 0 ] ; then
    echo "******** 发现错误：请查看并处理上述文件中未能正确导入的数据 ********"
  else
    echo "******** $datefolder 数据处理无异常 ********"
  fi
  echo
  cd $pwdfolder
fi
