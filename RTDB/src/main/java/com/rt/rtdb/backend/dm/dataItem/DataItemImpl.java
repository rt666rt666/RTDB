package com.rt.rtdb.backend.dm.dataItem;


import com.rt.rtdb.backend.common.SubArray;
import com.rt.rtdb.backend.dm.DataManagerImpl;
import com.rt.rtdb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    /**
     * DataItemImpl类表示数据项的实现
     */
    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw; // 当前数据项的字节数组
        this.oldRaw = oldRaw; // 旧的数据项字节数组备份
        ReadWriteLock lock = new ReentrantReadWriteLock(); // 创建读写锁
        rLock = lock.readLock(); // 读锁
        wLock = lock.writeLock(); // 写锁
        this.dm = dm; // DataManagerImpl对象
        this.uid = uid; // 数据项的唯一标识符
        this.pg = pg; // 页面对象
    }

    /**
     * 检查数据项是否有效
     * @return 如果数据项有效则返回true，否则返回false
     */
    public boolean isValid() {
        // 检查字节数组中表示有效性的位置是否为0
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    @Override
    public SubArray data() {
        // 返回数据项的数据部分的子数组
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock(); // 获取写锁
        pg.setDirty(true); // 设置页面为已修改状态
        // 备份当前数据项字节数组到旧的数据项字节数组
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        // 将旧的数据项字节数组还原到当前数据项字节数组
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock(); // 释放写锁
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this); // 在事务日志中记录数据项的操作
        wLock.unlock(); // 释放写锁
    }

    @Override
    public void release() {
        dm.releaseDataItem(this); // 释放数据项的资源
    }

    @Override
    public void lock() {
        wLock.lock(); // 获取写锁
    }

    @Override
    public void unlock() {
        wLock.unlock(); // 释放写锁
    }

    @Override
    public void rLock() {
        rLock.lock(); // 获取读锁
    }

    @Override
    public void rUnLock() {
        rLock.unlock(); // 释放读锁
    }

    @Override
    public Page page() {
        return pg; // 返回数据项所在的页面对象
    }

    @Override
    public long getUid() {
        return uid; // 返回数据项的唯一标识符
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw; // 返回旧的数据项字节数组
    }

    @Override
    public SubArray getRaw() {
        return raw; // 返回当前数据项的字节数组
    }

}
