# Spark 实时计算


## 软件版本

- sbt 1.2.1
- scala 2.11.12
- spark 2.2.x
- redis 4.x
- hbase 1.x.0
- kafka 1.x

## 代码结构

```
src/main/resources 配置文件
src/main/scala  scala 文件根目录

实时计算包
    com.jindanfenqi.spark: 
        models   实体对象包
        sources  数据源包
            kafka  kafka 数据源
            hbase  hbase 数据源
            redis  redis 数据源
            mysql  mysql 数据源
        stream   流式计算包
        utils    实用工具包
        conf     配置包
```


## 约定

```
1. redis key 命名 前缀：spark:指标名:具体名称
2. HBase 配置：
    1. 前缀 hbase
    2. 配置标识名称
    唯一标识： 前缀.配置标识
    3. 配置字段: catalog, newtable, checkpointLocation, interval
```