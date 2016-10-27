#!/bin/bash -e
#set -xv

# usage: sh importHbase.sh pathtoimported(要导入文件的目录名) reduceNum(reduce数量) tableName(导入的表名)"
# 用法举例：例如，从/stock/tbt_import导入到tbt_original的数据，其中将tbt_original已经预分区为33个。则importHbase.sh /stock/tbt_import 33 tbt_original"
# 数据量大的时候reduceNum应与hbase表的region个数相同，数据量不大时，数值随意，比如可以设为4。


############################   基本的集群配置和导入文件数量及大小   #################################
user=$(whoami)
# 集群配置
blockSize=128 #blockSize M为单位
nodeMem=39936 #分配给Yarn的单节点内存
nodeVcores=28 #分配给Yarn的vcore数量
nodeNum=8     #节点数
maxReduceMem=43520  #reduce最大值，这是在集群上设置的。
let totalMem=${nodeNum}*${nodeMem}
let totalVcores=${nodeNum}*${nodeVcores}
# 得到待导入文件的信息
fileSize=$(hdfs dfs -du -s  ${1}  |cut -d ' ' -f 1)
let fileSize=fileSize/1024/1024
fileNum=$(hdfs dfs -ls  ${1} | wc -l)
let fileNum=fileNum-1
# 如果是文件的话数量就是一个
if [ $fileNum -lt 1 ];then
	reduceMem=1
fi


############################# map 参数的计算，主要是为了算出$trueMapMemmb ##################
# 每个Vcore上运行一个map
maxParallelMapNum=$totalVcores
#1个Vcore上运行一个map，所以每个map的最大内存设为每个Vcore内存的75%，以便确保资源够用
let maxParallelMapMem=${nodeMem}/${nodeVcores}*75/100 
#根据miniSplit分割文件，得到map数量$numMaps
let minSplit=${maxParallelMapMem}*8*1024*1024/10
let numMaps=$fileSize*1024*1024/minSplit 
if  [ $numMaps -lt $fileNum ];then
	numMaps=$fileNum
fi
# 文件大小除以map个数，得到map的内存$trueMapMemmb
let trueMapMemmb=${fileSize}/${numMaps} 
# 确保map的内存在1G到$maxParallelMapNum之间
if [ ${trueMapMemmb} -lt 1024 ];then
	trueMapMemmb=1024
fi
if [ $trueMapMemmb -gt $maxParallelMapMem ];then
	trueMapMemmb=$maxParallelMapMem
fi
#map.java.opts.max.heap设为map.memory的80%
let truemapjava=$trueMapMemmb*8/10



###########################  reduce参数的计算，主要是计算$reduceVcores和$reduceVcores##########
reduceNum=$2
#根据文件大小和reduce数量确定reducememory，使它处于1G和最大值maxReduceMem之间
let reduceMem=${fileSize}/${reduceNum}*12/10  
if [ $reduceMem -lt 1024 ];then
	reduceMem=1024
fi
if [ $reduceMem -gt $maxReduceMem ];then 
	reduceMem=39168
fi
# 根据系统的总内存和reduceMem的大小算出每个Reduce Task需要的虚拟CPU个数
let MaxReduceNumPerNodereduce=${nodeMem}/${reduceMem} #依据节点内存和每个reduce内存来计算并行最大并行reduce数量
if [ $MaxReduceNumPerNodereduce -gt $nodeVcores ];then #当文件特别小的时候，依据内存计算出来的CPU数量大于了节点上的CPU数量。
	MaxReduceNumPerNodereduce=$nodeVcores  #让全部CPU用于计算
fi
let reduceVcores=${nodeVcores}/${MaxReduceNumPerNodereduce}
# reduce.java.opts.max.heap设为map.memory的80%
let reduceJava=${reduceMem}*8/10


uuiddir=$(cat /proc/sys/kernel/random/uuid)
tempdir=/user/${user}/${uuiddir}
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
-Dimporttsv.columns=HBASE_ROW_KEY,family:DATE,family:SourceCollectionIdentifier,family:SourceCommonName,family:DocumentIdentifier,family:Counts,family:V2Counts,family:Themes,family:V2Themes,family:Locations,family:V2Locations,family:Persons,family:V2Persons,family:Organizations,family:V2Organizations,family:V2Tone,family:Dates,family:GCAM,family:SharingImage,family:RelatedImages,family:SocialImageEmbeds,family:SocialVideoEmbeds,family:Quotations,family:AllNames,family:Amounts,family:TranslationInfo,family:Extras \
-Dimporttsv.bulk.output=$tempdir \
-Dmapred.min.split.size=$minSplit \
-Ddfs.umaskmode=000 \
-Dmapreduce.map.memory.mb=$trueMapMemmb  \
-Dmapreduce.map.java.opts.max.heap=$truemapjava \
-Dmapreduce.reduce.memory.mb=$reduceMem  \
-Dmapreduce.reduce.java.opts.max.heap=$reduceJava \
-Dmapreduce.reduce.cpu.vcores=$reduceVcores \
 $3  $1
sudo -u hdfs hdfs dfs -chown -R hbase:hbase ${tempdir}
sudo -u hdfs hdfs dfs -mv $tempdir /
sudo -u hbase hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /$uuiddir $3
sudo -u hdfs hdfs dfs -rm -r /$uuiddir

