# RTDB

RTDB 是一个 Java 实现的简单的数据库，做这个项目主要是想加强对数据库原理的理解，主要参考 MySQL 数据库的设计原理，实现数据存储、事务管理、日志记录、MVCC、索引检索等，RTDB 分为后端和前端（客户端），前后端通过 socket 进行交互。


## 工具库： JUnit、Guava、Gson、Commons Codec、Commons CLI
## 主要实现功能
- 实现事务状态，并通过接口查询事务状态；
- 实现基于 B+树的聚簇索引，支持基于索引查找数据。
- 实现对字段结构和表结构的管理。实现 SQL 语句解析，并根据语句操作表。
- 自定义引用计数缓存框架，对分页管理和数据项管理提供缓存支持。
- 实现数据库日志管理，保证发生错误时可以根据日志进行恢复，进而保证数据一致性。
- 基于两段锁协议实现调度序列的可串行化，并实现 MVCC 以消除读写阻塞。同时实现两种隔离级别。

## 运行方式

注意首先需要在 pom.xml 中调整编译版本，如果导入 IDE，请更改项目的编译版本以适应你的 JDK

首先执行以下命令编译源码：

```shell
mvn compile
```

接着执行以下命令以 /tmp/rtdb 作为路径创建数据库：

```shell
mvn exec:java -Dexec.mainClass="com.rt.rtdb.backend.Launcher" -Dexec.args="-create /tmp/rtdb"
```

随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java -Dexec.mainClass="com.rt.rtdb.backend.Launcher" -Dexec.args="-open /tmp/rtdb"
```

这时数据库服务就已经启动在本机的 9999 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java -Dexec.mainClass="com.rt.rtdb.client.Launcher"
```

会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。

一个执行示例：
![za9vib.png](https://files.catbox.moe/za9vib.png)

![wv3jc8.png](https://files.catbox.moe/wv3jc8.png)

