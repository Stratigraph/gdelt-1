#/bin/bash
# 函数在执行之前必须定义，不能预编译，所以执行语句在最后
function import2hdfs(){
    # 参数顺序 $datatype $filedir $filename $hdfspath $logdir
    # hdfs 的相关操作
    hdfs dfs -appendToFile  ${2}${3} ${4}
    if [ $? -eq 0 ]; then
        echo ${3}" to "${4} >>  ${5}${1}".hdfs"
        # 把源csv文件删掉
        rm $2$3
        return 0;
    else
        echo ${3}" to "${4} >>  ${5}${1}".failhdfs"
        return 1;
    fi
}
function import2hbase(){
    # 参数顺序datatype filename shselection hdfstmp reducenum hbasetable logdir
    # hbase相关操作
    # 进入工作目录
    cd $HOME"/blmbigdata/jobs"
    sh ${3} ${4} ${5} ${6}
    if [ $? -eq 0 ]; then
        echo $f{2} >>  ${7}${1}".hbase"
        return 0
    else
        echo $f{2} >>  ${7}${1}".failhabse"
        return 1
    fi
}
function main(){
    hdfspathhead="/gdelt/"
    filedir=$HOME"/gdelttmpdata/"$1"/"  # 原始文件目录
    logdir=$HOME"/gdelttmpdata/log/"     # 倒入日志的目录
    # 需要确定的一些参数
    hbasetable=""    # hbase表名
    shselection=""   # 倒入hbase表所用到的脚本名称
    reducenum=""     # 倒入所需reduce的数量
    cd $filedir      # 切换到待导入文件的目录

    # 得到本次需要处理的文件数组tobeimport
    j=0
    for i in `ls -1 | grep "zip"`
    do
        # 对要处理的文件进行筛选，主要是防止上一次任务还没结束但已经提交的文件被重复处理
        if [ `grep -c $i $logdir"processedfile"` -eq 0 ]; then
            tobeimport[j]=$i
            j=`expr $j + 1`
            echo $i >> $logdir"processedfile"
        fi
    done

    if [ ${#tobeimport[@] -gt 0} ]; then
        for name in ${tobeimport[*]}
        do
            # 保证工作目录
            cd $filedir
            # 读取压缩文件
            filename=${name%.zip*}
            unzip $name
            if [ $? -eq 1 ]; then
                echo $name >> $logdir$1".badfile"
            fi
            # 根据文件类型确定相应参数
            hdfsfilename=${filename:0:4}".csv"
            # 用于倒入hbase的临时文件，导入后删除
            hdfstmp="/gdelttmp/"`date +%H%I%M`".csv"
            if [ $1 == "gkg" ]; then
                if [[ $filename =~ "gkgcounts" ]]; then
                    hdfspath=$hdfspathhead"v1/gkgcounts/"$hdfsfilename
                    hbasetable="GDELT_GKGCOUNTS1"
                    shselection=""
                    reducenum=""
                else
                    hdfspath=$hdfspathhead"v1/gkg/"$hdfsfilename
                    hbasetable="GDELT_GKG1"
                    shselection=""
                    reducenum=""
                fi
                # 这两类文件的第一行是字段名，要删掉
                sed -i '1d' $filename
                if [ $? -eq 1 ]; then
                    echo $filename >> $logdir"fail.sed1d"$1
                fi

            elif [ $1 == "events" ]; then  # v1的event
                hdfspath=$hdfspathhead"v1/events/"$hdfsfilename
                hbasetable="GDELT_EVENTS1"
                shselection=""
                reducenum="18"

            else # v2
                if [[ $filename =~ "gkg" ]]; then
                    hdfspath=$hdfspathhead"v2/gkg/"$hdfsfilename
                    hbasetable="GDELT_GKG2_TEST"
                    shselection="importGdeltV2gkg.sh"
                    reducenum="1"
                # events的文件名是export.CSV
                elif [[ $filename =~ "export" ]]; then
                    hdfspath=$hdfspathhead"v2/events/"$hdfsfilename
                    hbasetable="GDELT_EVENTS2_TEST"
                    shselection="importGdeltV2Event.sh"
                    reducenum="5"

                else
                    hdfspath=$hdfspathhead"v2/mentions/"$hdfsfilename
                    hbasetable="GDELT_MENTIONS2_TEST"
                    shselection="importGdeltV2mentions.sh"
                    reducenum="7"
                fi
            fi
            # 与之前的文件合并
            import2hdfs $1 $filedir $filename $hdfspath $logdir
            # 生成一个临时文件，以便倒入hbase
            import2hdfs $1 $filedir $filename $hdfstmp $logdir
            # 倒入hbase
            import2hbase $1 $filename $shselection $hdfstmp $reducenum $hbasetable $logdir
            if [ $? -eq 0 ]; then
                hdfs dfs -rm $hdfstmp
            fi
            # 把源zip文件删掉
            rm $filedir$name 
        done
    fi
}
# 执行主函数，只执行v2的，目前不能执行events(v1的events)和gkg(v1的gkg和gkgcounts)
main v2

