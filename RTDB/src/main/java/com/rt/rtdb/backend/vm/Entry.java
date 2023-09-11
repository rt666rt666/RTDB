package com.rt.rtdb.backend.vm;

import com.google.common.primitives.Bytes;
import com.rt.rtdb.backend.common.SubArray;
import com.rt.rtdb.backend.dm.dataItem.DataItem;
import com.rt.rtdb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [DATA]
 * XMIN 创建该版本的事务编号
 * XMAX 删除该版本的事务编号
 * DATA 是这条记录持有的数据
 */
public class Entry {

    // 记录中XMIN的偏移量
    private static final int OF_XMIN = 0;

    // 记录中XMAX的偏移量
    private static final int OF_XMAX = OF_XMIN+8;

    // 记录中data的偏移量
    private static final int OF_DATA = OF_XMAX+8;

    // 记录的唯一标识符
    private long uid;

    // 记录的数据项
    private DataItem dataItem;

    // 版本管理器
    private VersionManager vm;

    /**
     * 创建新的记录对象
     *
     * @param vm        版本管理器
     * @param dataItem  数据项
     * @param uid       记录的唯一标识符
     * @return          新的记录对象
     */
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    /**
     * 从数据库中加载指定UID的记录
     *
     * @param vm    版本管理器
     * @param uid   记录的唯一标识符
     * @return      加载的记录对象
     * @throws Exception    异常
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    /**
     * 将事务ID和数据内容包装成字节数组形式的记录
     *
     * @param xid   事务ID
     * @param data  数据内容
     * @return      包装后的字节数组记录
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    /**
     * 释放记录，使其可被其他事务访问
     */
    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    /**
     * 移除记录
     */
    public void remove() {
        dataItem.release();
    }

    /**
     * 以拷贝的形式返回数据内容
     *
     * @return 数据内容的拷贝
     */
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取记录的最早事务ID
     *
     * @return 记录的最早事务ID
     */
    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取记录的最新事务ID
     *
     * @return 记录的最新事务ID
     */
    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置记录的最新事务ID
     *
     * @param xid   最新事务ID
     */
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    /**
     * 获取记录的唯一标识符
     *
     * @return 记录的唯一标识符
     */
    public long getUid() {
        return uid;
    }
}
