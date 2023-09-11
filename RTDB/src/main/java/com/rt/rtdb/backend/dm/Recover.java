package com.rt.rtdb.backend.dm;

import com.google.common.primitives.Bytes;
import com.rt.rtdb.backend.common.SubArray;
import com.rt.rtdb.backend.dm.dataItem.DataItem;
import com.rt.rtdb.backend.dm.logger.Logger;
import com.rt.rtdb.backend.dm.page.Page;
import com.rt.rtdb.backend.dm.page.PageX;
import com.rt.rtdb.backend.dm.pageCache.PageCache;
import com.rt.rtdb.backend.tm.TransactionManager;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.backend.utils.Parser;

import java.util.*;
import java.util.Map.Entry;
/**
 * 恢复日志
 * @author ryh
 * @version 1.0
 * @since 1.0
 * @create 2023/7/15 17:07
 **/
public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("恢复中...");
        // 将日志指针倒回到起始位置
        lg.rewind();
        int maxPgno = 0;
        while(true) {
            // 逐条读取日志
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                // 解析插入日志
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                // 解析更新日志
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        // 截断页缓存
        pc.truncateByBgno(maxPgno);
        System.out.println("截断至 " + maxPgno + " 页");
        // 重做事务
        redoTransactions(tm, lg, pc);
        System.out.println("重做事务完成");
        // 撤销事务
        undoTransactions(tm, lg, pc);
        System.out.println("撤销事务完成");

        System.out.println("恢复完成");
    }

    // 重做事务的方法
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        // 将日志指针倒回到起始位置
        lg.rewind();
        while (true) {
            // 逐条读取日志
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                // 解析插入日志
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (!tm.isActive(xid)) {
                    // 如果事务不处于活动状态，则执行重做插入日志操作
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 解析更新日志
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (!tm.isActive(xid)) {
                    // 如果事务不处于活动状态，则执行重做更新日志操作
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    /**
     * 撤销所有事务
     * @param tm 事务管理器
     * @param lg 日志记录器
     * @param pc 页面缓存对象
     */
    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        // 创建日志缓存，用于按事务 ID 存储相关日志
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        // 将日志记录器回溯到起始位置
        lg.rewind();
        // 循环读取日志
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                // 如果是插入日志
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (tm.isActive(xid)) {
                    // 如果事务是活动状态，则将日志添加到对应事务的日志列表中
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                // 如果是更新日志
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (tm.isActive(xid)) {
                    // 如果事务是活动状态，则将日志添加到对应事务的日志列表中
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有活动事务的日志进行倒序撤销操作
        for (Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            //获取日志列表
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    // 如果是插入日志，则执行插入日志的撤销操作
                    doInsertLog(pc, log, UNDO);
                } else {
                    // 执行更新日志的撤销操作
                    doUpdateLog(pc, log, UNDO);
                }
            }
            // 中止事务
            tm.abort(entry.getKey());
        }
    }


    /**
     * 判断日志是否为插入日志
     * @param log 日志字节数组
     * @return 如果是插入日志返回true，否则返回false
     */
    private static boolean isInsertLog(byte[] log) {
        // 判断日志类型是否为插入日志
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 解析更新日志的相关信息
     * @param log 日志数据
     * @return 更新日志信息对象
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo(); // 创建更新日志信息对象
        // 解析 xid 字段
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        // 解析 uid 字段
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1)); // 计算 offset 值
        uid >>>= 16; // 将 uid 右移 16 位
        li.pgno = (int)(uid & ((1L << 32) - 1)); // 计算 pgno 值
        int length = (log.length - OF_UPDATE_RAW) / 2; // 计算原始数据长度
        // 提取旧的原始数据
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        // 提取新的原始数据
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li; // 返回更新日志信息对象
    }


    /**
     * 执行更新日志的操作（重做或撤销）
     * @param pc 页面缓存对象
     * @param log 日志数据
     * @param flag 操作标志，REDO 表示重做操作，UNDO 表示撤销操作
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if (flag == REDO) {
            // 重做操作，解析更新日志的相关信息
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            // 撤销操作，解析更新日志的相关信息
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }

        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            // 调用 PageX 类的 recoverUpdate() 方法执行更新操作
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }


    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    /**
     * 插入日志
     * @param xid 事务ID
     * @param pg 页面对象
     * @param raw 原始数据
     * @return 插入的日志数据
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT}; // 日志类型为插入
        byte[] xidRaw = Parser.long2Byte(xid); // 将事务ID转换为字节数组
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber()); // 将页面号转换为字节数组
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg)); // 将页面中的FSO（Free Space Offset）转换为字节数组
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw); // 拼接日志数据并返回
    }

    /**
     * 解析插入日志
     * @param log 日志字节数组
     * @return 解析后的插入日志信息
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        // 解析事务 ID
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        // 解析页号
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        // 解析偏移量
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        // 解析原始数据
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    /**
     * 执行插入日志操作
     * @param pc 页面缓存对象
     * @param log 日志字节数组
     * @param flag 操作标志，指示是重做操作还是撤销操作
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            // 根据页号获取对应的页面
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            // 发生异常时触发恐慌
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                // 如果是撤销操作，则将数据项标记为无效
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 执行页面的插入恢复操作
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            // 释放页面资源
            pg.release();
        }
    }

}
