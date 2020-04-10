#!/bin/sh

hive_server="yourip"
thrift_port="yourport"
action=$1
home_dir=/home/hadoop/tmp/test_hive_perf

usage(){
    echo -e "usage:\n"
    echo -e "./batchtbls.sh createdb yourdb\n" 
    echo -e "./batchtbls.sh dropdb yourdb\n" 
    echo -e "./batchtbls.sh showdbs\n" 
    echo -e "./batchtbls.sh createdbs dbs_cnt\n" 
    echo -e "./batchtbls.sh createtbls yourdb tbls_cnt_start tbls_cnt_end parallel\n" 
    echo -e "./batchtbls.sh showtbls yourdb\n"
}
if [ "$1" = "" ];then
    usage
    exit 0
fi

if [ "$1" = "-h" -o "$1" = "--help" ];then
    usage
    exit 0
fi

#####################数据库相关操作#####################
if [ $1 = "dropdb" ];then
    database=$2
    beeline -u jdbc:hive2://${hive_server}:${thrift_port} -n hadoop -p hadoop -e 'drop database if exists '${database}' cascade;'
    exit 0
elif [ $1 = "createdb" ];then
    database=$2
    beeline -u jdbc:hive2://${hive_server}:${thrift_port} -n hadoop -p hadoop -e 'create database if not exists '${database}';'
    exit 0
elif [ $1 = "showdbs" ];then
    beeline -u jdbc:hive2://${hive_server}:${thrift_port} -n hadoop -p hadoop -e 'show databases;'
    exit 0
elif [ $1 = "createdbs" ];then
    dbs_cnt=$2
    
    if [ -f "dbs.hql" ];then
        rm -f dbs.hql
    fi

    for i in $(seq 1 $dbs_cnt)
    do
        echo -e "create database if not exists testdb_$i; \n\n" >> dbs.hql
    done

    echo -e "生成${dbs_cnt}个建库语句dbs.hql文件\n"
    echo -e "向Hive中建${dbs_cnt}个库将在5秒后开始......\n"
    sleep 5

    starttime=`date +'%Y-%m-%d %H:%M:%S'`
    beeline -u jdbc:hive2://${hive_server}:${thrift_port} -n hadoop -p hadoop -f /home/hadoop/tmp/test_hive_perf/dbs.hql >> /home/hadoop/tmp/test_hive_perf/logs/create_${dbs_cnt}_dbs.log
    endtime=`date +'%Y-%m-%d %H:%M:%S'`
    start_seconds=$(date --date="$starttime" +%s);
    end_seconds=$(date --date="$endtime" +%s);
    echo -e "\n向Hive(${hive_server}:${thrift_port})中创建${dbs_cnt}个库，运行时间："$((end_seconds-start_seconds))"s\n"

    exit 0
fi


#####################表相关操作##########################
database=$2

if [ $1 = "showtbls" ];then
    beeline -u jdbc:hive2://${hive_server}:${thrift_port} -n hadoop -p hadoop -e 'use '${database}'; show tables;'
    exit 0
fi

if [ $1 != "createtbls" ];then
    usage
    exit 0
fi

tbls_cnt_start=$3
tbls_cnt_end=$4
tbls_cnt=$((tbls_cnt_end-tbls_cnt_start+1))
parallel=$5
block_size=$((tbls_cnt/parallel))

finish_cnt=0
now=`date +'%Y%m%d-%H%M%S'`

stmt_create=" create table "
stmt_fields=" ( `cat ./fields.txt` ) "
stmt_remain=" comment 'user info 用户信息' row format delimited fields terminated by ','; "

#echo -e "$stmt_create tbl $stmt_fields $stmt_remain \n\n"

if [ ! -d "${home_dir}/logs/createtbls_${database}" ];then
    mkdir -p ${home_dir}/logs/createtbls_${database}
fi
if [ ! -d "${home_dir}/hqls/createtbls_${database}" ];then
    mkdir -p ${home_dir}/hqls/createtbls_${database}
fi

createtbls_log_file=$home_dir/logs/createtbls_${database}/create_${tbls_cnt_start}to${tbls_cnt_end}_tbls_${now}.log

function createtbls(){
    createtbls_start=$1
    createtbls_end=$2
    createtbls_cnt=$((createtbls_end-createtbls_start+1))
    starttime=`date +'%Y-%m-%d %H:%M:%S'`
    tbls_hql_file=${home_dir}/hqls/createtbls_${database}/tbls${createtbls_start}to${createtbls_end}.hql
    echo -e "\n准备提交任务! 向Hive(${hive_server}:${thrift_port}/${database})中创建${createtbls_start}到${createtbls_end}共${createtbls_cnt}张表!\n"
    beeline -u jdbc:hive2://${hive_server}:${thrift_port}/${database} -n hadoop -p hadoop --silent=true -f ${tbls_hql_file} >>/dev/null 
    endtime=`date +'%Y-%m-%d %H:%M:%S'`
    start_seconds=$(date --date="$starttime" +%s);
    end_seconds=$(date --date="$endtime" +%s);
    echo -e "\n已完成! 向Hive(${hive_server}:${thrift_port}/${database})中创建${createtbls_start}到${createtbls_end}共${createtbls_cnt}张表，运行时间："$((end_seconds-start_seconds))"s!\n" >> ${createtbls_log_file}
    echo -e "\n已完成! 向Hive(${hive_server}:${thrift_port}/${database})中创建${createtbls_start}到${createtbls_end}共${createtbls_cnt}张表，运行时间："$((end_seconds-start_seconds))"s!\n"
    finish_cnt=$((finish_cnt+1))
}

function buildhqls(){
    buildhql_start=$1
    buildhql_end=$2
    buildhql_cnt=$((buildhql_end-buildhql_start+1))
    
    tbls_hql_file=${home_dir}/hqls/createtbls_${database}/tbls${buildhql_start}to${buildhql_end}.hql
    if [ -f ${tbls_hql_file} ];then
        rm -f $tbls_hql_file
    fi
    echo -e "生成${buildhql_start}到${buildhql_end}共${buildhql_cnt}条建表语句${tbls_hql_file}文件\n"
    for i in $(seq $buildhql_start $buildhql_end)
    do
        echo -e "$stmt_create testtbl_$i $stmt_fields $stmt_remain \n\n" >> ${tbls_hql_file}
    done
}

block_start=$tbls_cnt_start
block_end=$block_size
for((;$block_end <= $tbls_cnt_end;));
do
    echo "$block_start $block_end \n"
       
    buildhqls $block_start $block_end

    block_start=$((block_end+1))
    block_end=$((block_end+block_size))
done

echo -e "向Hive中创建${tbls_cnt_start}到${tbls_cnt_end}共${tbls_cnt}张表${parallel}并发度，将在3秒后开始......\n"
sleep 3

block_start=$tbls_cnt_start
block_end=$block_size
for((;$block_end <= $tbls_cnt_end;));
do
    echo "$block_start $block_end \n"
    createtbls $block_start $block_end &
    block_start=$((block_end+1))
    block_end=$((block_end+block_size))
    sleep 10
done

echo "已提交${parallel}个任务给beeline!"
