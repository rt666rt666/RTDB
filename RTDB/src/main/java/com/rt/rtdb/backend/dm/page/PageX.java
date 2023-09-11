package com.rt.rtdb.backend.dm.page;

import com.rt.rtdb.backend.dm.pageCache.PageCache;
import com.rt.rtdb.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {

    private static final short OF_FREE = 0;  // 空闲位置开始偏移的起始位置
    private static final short OF_DATA = 2;  // 数据起始位置
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;  // 最大空闲空间大小

    /**
     * 初始化一个原始的页面
     * @return 初始化后的页面
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);  // 初始化页面并设置空闲位置开始偏移
        return raw;
    }

    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);  // 设置空闲位置开始偏移
    }

    /**
     * 获取页面的空闲位置开始偏移
     * @param pg 页面对象
     * @return 空闲位置开始偏移
     */
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());  // 获取页面的空闲位置开始偏移
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));  // 解析空闲位置开始偏移
    }

    /**
     * 将原始数据插入页面，并返回插入位置
     * @param pg 页面对象
     * @param raw 原始数据
     * @return 插入位置
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);  // 设置页面为脏页
        short offset = getFSO(pg.getData());  // 获取空闲位置开始偏移
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);  // 将数据插入页面
        setFSO(pg.getData(), (short)(offset + raw.length));  // 更新空闲位置开始偏移
        return offset;  // 返回插入位置
    }

    /**
     * 获取页面的空闲空间大小
     * @param pg 页面对象
     * @return 空闲空间大小
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());  // 计算页面的空闲空间大小
    }

    /**
     * 将原始数据插入页面的指定位置，并将页面的偏移设置为较大的偏移
     * @param pg 页面对象
     * @param raw 原始数据
     * @param offset 插入位置
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);  // 设置页面为脏页
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);  // 将数据插入页面

        short rawFSO = getFSO(pg.getData());  // 获取空闲位置开始偏移
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));  // 更新空闲位置开始偏移
        }
    }

    /**
     * 将原始数据插入页面的指定位置，不更新偏移
     * @param pg 页面对象
     * @param raw 原始数据
     * @param offset 插入位置
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);  // 设置页面为脏页
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);  // 将数据插入页面
    }
}
