package com.rt.rtdb.backend.vm;

import com.rt.rtdb.backend.tm.TransactionManager;

/**
 * 可见性类，用于判断版本是否跳过和记录是否可见。
 */
public class Visibility {

    /**
     * 判断版本是否存在版本跳跃
     * @param tm 事务管理器
     * @param t 当前事务
     * @param e 记录
     * @return 如果版本跳过，则返回true；否则返回false。
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax(); // 获取删除该版本的事务编号
        if(t.level == 0) {
            return false; // 读提交是允许版本跳跃的，而可重复读则是不允许版本跳跃的
        } else {
            // 判断版本是否已提交且大于当前事务的版本或在当前事务的快照中
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /**
     * 判断记录是否可见。
     * @param tm 事务管理器
     * @param t 当前事务
     * @param e 记录
     * @return 如果记录可见，则返回true；否则返回false。
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e); // 使用读已提交隔离级别判断记录是否可见
        } else {
            return repeatableRead(tm, t, e); // 使用可重复读隔离级别判断记录是否可见
        }
    }

    /**
     * 使用读已提交隔离级别判断记录是否可见。
     * @param tm 事务管理器
     * @param t 当前事务
     * @param e 记录
     * @return 如果记录可见，则返回true；否则返回false。
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid; // 当前事务的版本号
        long xmin = e.getXmin(); // 创建该版本的事务编号
        long xmax = e.getXmax(); // 删除该版本的事务编号
        if(xmin == xid && xmax == 0) return true; // 记录是当前事务创建的，且没有被删除

        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true; // 记录已提交，且没有被删除
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true; // 记录已提交，但是不是当前事务的版本
                }
            }
        }
        return false; // 记录不可见
    }

    /**
     * 使用可重复读隔离级别判断记录是否可见。
     * @param tm 事务管理器
     * @param t 当前事务
     * @param e 记录
     * @return 如果记录可见，则返回true；否则返回false。
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid; // 当前事务的版本号
        long xmin = e.getXmin(); // 创建该版本的事务编号
        long xmax = e.getXmax(); // 删除该版本的事务编号
        if(xmin == xid && xmax == 0) return true; // 记录是当前事务创建的，且没有被删除

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true; // 记录已提交，且没有被删除
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true; // 记录已提交，但是不是当前事务的版本或在当前事务的快照中
                }
            }
        }
        return false; // 记录不可见
    }

}
