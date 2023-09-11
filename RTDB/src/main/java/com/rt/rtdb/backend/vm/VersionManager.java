package com.rt.rtdb.backend.vm;

import com.rt.rtdb.backend.dm.DataManager;
import com.rt.rtdb.backend.tm.TransactionManager;

public interface VersionManager {
    // 读取指定事务和数据版本的数据
    byte[] read(long xid, long uid) throws Exception;

    // 插入数据并返回新版本的唯一标识符
    long insert(long xid, byte[] data) throws Exception;

    // 删除指定事务和数据版本的数据
    boolean delete(long xid, long uid) throws Exception;

    // 开始一个新的事务，并指定事务隔离级别
    long begin(int level);

    // 提交指定事务
    void commit(long xid) throws Exception;

    // 中止指定事务
    void abort(long xid);

    // 创建一个新的VersionManager实例
    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}

