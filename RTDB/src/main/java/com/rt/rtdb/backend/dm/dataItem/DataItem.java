package com.rt.rtdb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.rt.rtdb.backend.common.SubArray;
import com.rt.rtdb.backend.dm.DataManagerImpl;
import com.rt.rtdb.backend.dm.page.Page;
import com.rt.rtdb.backend.utils.Parser;
import com.rt.rtdb.backend.utils.Types;
import java.util.Arrays;

public interface DataItem {
    SubArray data();
    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();
    /**
     * 将原始数据包装为DataItem的字节数组形式
     * @param raw 原始数据
     * @return DataItem的字节数组形式
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1]; // 创建一个字节数组，用于表示数据项的有效性
        byte[] size = Parser.short2Byte((short)raw.length); // 将原始数据的长度转换为字节数组
        return Bytes.concat(valid, size, raw); // 将有效性、长度和原始数据拼接为一个新的字节数组
    }

    /**
     * 从页面的offset处解析出DataItem对象
     * @param pg 页面对象
     * @param offset 偏移量
     * @param dm DataManagerImpl对象
     * @return 解析出的DataItem对象
     */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData(); // 获取页面的原始数据字节数组
        // 从原始数据中解析出数据项的大小
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE,
                offset+DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA); // 计算数据项的总长度
        long uid = Types.addressToUid(pg.getPageNumber(), offset); // 根据页面号和偏移量计算数据项的唯一标识符
        // 创建并返回DataItemImpl对象
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    /**
     * 将DataItem的字节数组标记为无效
     * @param raw DataItem的字节数组
     */
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1; // 将字节数组中表示有效性的位置标记为无效状态
    }

}
