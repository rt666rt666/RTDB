package com.rt.rtdb.backend.vm;

import com.rt.rtdb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * vm对一个事务的抽象
 * @author ryh
 * @version 1.0
 * @since 1.0
 * @create 2023/8/4 10:52
 **/
public class Transaction {
    public long xid; // 事务ID
    public int level; // 事务隔离级别
    public Map<Long, Boolean> snapshot; // 事务快照
    public Exception err; // 异常信息
    public boolean autoAborted; // 是否自动中止事务

    /**
     * 创建一个新的事务
     *
     * @param xid    事务ID
     * @param level  隔离级别
     * @param active 活跃事务的映射表
     * @return 新创建的事务
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid; // 设置事务ID
        t.level = level; // 设置隔离级别

        if (level != 0) {
            t.snapshot = new HashMap<>(); // 创建一个新的快照
            // 为快照添加活跃事务
            for (Long x : active.keySet()) {
                t.snapshot.put(x, true); // 将活跃事务添加到快照中
            }
        }
        return t; // 返回新创建的事务
    }

    /**
     * 判断事务是否在快照中
     *
     * @param xid 事务ID
     * @return 如果事务在快照中，则返回true；否则返回false
     */
    public boolean isInSnapshot(long xid) {
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false; // 如果事务ID为特殊值SUPER_XID，则不在快照中，返回false
        }
        return snapshot.containsKey(xid); // 检查快照中是否包含给定的事务ID，并返回结果
    }
}

