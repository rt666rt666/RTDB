package com.rt.rtdb.backend.vm;

import com.rt.rtdb.backend.common.AbstractCache;
import com.rt.rtdb.backend.dm.DataManager;
import com.rt.rtdb.backend.tm.TransactionManager;
import com.rt.rtdb.backend.tm.TransactionManagerImpl;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 版本管理器的实现类。
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm; // 事务管理器
    DataManager dm; // 数据管理器
    Map<Long, Transaction> activeTransaction; // 活跃事务列表
    Lock lock; // 用于并发控制的锁
    LockTable lt; // 锁表

    /**
     * 构造函数
     * @param tm 事务管理器
     * @param dm 数据管理器
     */
    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>(); // 初始化活跃事务列表
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null)); // 添加超级事务到活跃事务列表
        this.lock = new ReentrantLock(); // 初始化锁
        this.lt = new LockTable(); // 初始化锁表
    }

    /**
     * 读取数据
     * @param xid 事务ID
     * @param uid 记录ID
     * @return 读取到的数据
     * @throws Exception 如果事务发生错误
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock(); // 加锁
        Transaction t = activeTransaction.get(xid); // 获取事务
        lock.unlock(); // 解锁

        if(t.err != null) { // 如果事务发生错误
            throw t.err; // 抛出异常
        }

        Entry entry = null;
        try {
            entry = super.get(uid); // 从缓存中获取记录
        } catch(Exception e) {
            if(e == Error.NullEntryException) { // 如果记录不存在
                return null; // 返回空
            } else {
                throw e; // 抛出异常
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) { // 如果记录可见
                return entry.data(); // 返回数据
            } else {
                return null; // 否则返回空
            }
        } finally {
            entry.release(); // 释放记录
        }
    }

    /**
     * 插入数据
     * @param xid 事务ID
     * @param data 要插入的数据
     * @return 插入的数据的UID
     * @throws Exception 如果事务发生错误
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock(); // 加锁
        Transaction t = activeTransaction.get(xid); // 获取事务
        lock.unlock(); // 解锁

        if(t.err != null) { // 如果事务发生错误
            throw t.err; // 抛出异常
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data); // 封装记录数据
        return dm.insert(xid, raw); // 插入数据
    }

    /**
     * 删除数据
     * @param xid 事务ID
     * @param uid 要删除的数据的UID
     * @return 如果删除成功返回true，否则返回false
     * @throws Exception 如果事务发生错误
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock(); // 加锁
        Transaction t = activeTransaction.get(xid); // 获取事务
        lock.unlock(); // 解锁

        if(t.err != null) { // 如果事务发生错误
            throw t.err; // 抛出异常
        }
        Entry entry = null;
        try {
            entry = super.get(uid); // 从缓存中获取记录
        } catch(Exception e) {
            if(e == Error.NullEntryException) { // 如果记录不存在
                return false; // 返回false
            } else {
                throw e; // 抛出异常
            }
        }
        try {
            if(!Visibility.isVisible(tm, t, entry)) { // 如果记录不可见
                return false; // 返回false
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid); // 尝试在锁表中添加锁
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException; // 并发更新异常
                internAbort(xid, true); // 中止事务
                t.autoAborted = true;
                throw t.err; // 抛出异常
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) { // 如果记录已被删除
                return false; // 返回false
            }

            if(Visibility.isVersionSkip(tm, t, entry)) { // 如果版本跳跃
                t.err = Error.ConcurrentUpdateException; // 并发更新异常
                internAbort(xid, true); // 中止事务
                t.autoAborted = true;
                throw t.err; // 抛出异常
            }

            entry.setXmax(xid); // 设置记录的xmax为当前事务ID
            return true; // 返回true

        } finally {
            entry.release(); // 释放记录
        }
    }

    /**
     * 开始事务。
     * @param level 事务隔离级别
     * @return 开始的事务ID
     */
    @Override
    public long begin(int level) {
        lock.lock(); // 加锁
        try {
            long xid = tm.begin(); // 开始事务
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction); // 创建新事务
            activeTransaction.put(xid, t); // 将事务添加到活跃事务列表
            return xid; // 返回事务ID
        } finally {
            lock.unlock(); // 解锁
        }
    }

    /**
     * 提交事务。
     * @param xid 要提交的事务ID
     * @throws Exception 如果事务发生错误
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock(); // 加锁
        Transaction t = activeTransaction.get(xid); // 获取事务
        lock.unlock(); // 解锁

        try {
            if(t.err != null) { // 如果事务发生错误
                throw t.err; // 抛出异常
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock(); // 加锁
        activeTransaction.remove(xid); // 从活跃事务列表中移除事务
        lock.unlock(); // 解锁

        lt.remove(xid); // 从锁表中移除事务
        tm.commit(xid); // 提交事务
    }

    /**
     * 中止事务。
     * @param xid 要中止的事务ID
     */
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 内部中止事务的方法。
     * @param xid 要中止的事务ID
     * @param autoAborted 是否自动中止
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock(); // 加锁
        Transaction t = activeTransaction.get(xid); // 获取事务
        if(!autoAborted) {
            activeTransaction.remove(xid); // 从活跃事务列表中移除事务
        }
        lock.unlock(); // 解锁

        if(t.autoAborted) {
            return;
        }
        lt.remove(xid); // 从锁表中移除事务
        tm.abort(xid); // 中止事务
    }

    /**
     * 释放缓存中的记录。
     * @param entry 要释放的记录。
     */
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    /**
     * 根据给定的UID从缓存中检索记录。
     * @param uid 要检索的记录的UID。
     * @return 检索到的记录。
     * @throws Exception 如果记录为空。
     */
    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    /**
     * 释放缓存中的记录。
     * @param entry 要释放的记录。
     */
    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

}