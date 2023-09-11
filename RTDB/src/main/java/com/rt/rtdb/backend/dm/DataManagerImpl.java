package com.rt.rtdb.backend.dm;

import com.rt.rtdb.backend.common.AbstractCache;
import com.rt.rtdb.backend.dm.dataItem.DataItem;
import com.rt.rtdb.backend.dm.dataItem.DataItemImpl;
import com.rt.rtdb.backend.dm.logger.Logger;
import com.rt.rtdb.backend.dm.page.Page;
import com.rt.rtdb.backend.dm.page.PageOne;
import com.rt.rtdb.backend.dm.page.PageX;
import com.rt.rtdb.backend.dm.pageCache.PageCache;
import com.rt.rtdb.backend.dm.pageIndex.PageIndex;
import com.rt.rtdb.backend.dm.pageIndex.PageInfo;
import com.rt.rtdb.backend.tm.TransactionManager;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.backend.utils.Types;
import com.rt.rtdb.common.Error;

/**
 * @author RT666
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    /**
     * 读取数据项
     *
     * @param uid 唯一标识符
     * @return DataItem对象，如果数据项无效则返回null
     * @throws Exception 如果读取过程中发生异常
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid); // 从基类中获取DataItemImpl对象
        if (!di.isValid()) { // 检查数据项是否有效
            di.release(); // 释放无效的数据项
            return null;
        }
        return di; // 返回有效的数据项
    }

    /**
     * 插入数据项
     *
     * @param xid  事务ID
     * @param data 要插入的数据
     * @return 插入的数据项的唯一标识符
     * @throws Exception 如果插入过程中发生异常
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data); // 将数据包装为DataItem的字节数组形式
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException; // 如果数据项的大小超过页面的最大可用空间，则抛出异常
        }

        // 尝试获取可用页
        PageInfo pi = null;
        for (int i = 0; i < 5; i++) {
            pi = pIndex.select(raw.length); // 选择满足空间大小要求的页面
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw()); // 创建一个新页面
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE); // 将新页面添加到页面索引中
            }
        }
        if (pi == null) {
            throw Error.DatabaseBusyException; // 如果无法获取满足条件的页面，则抛出数据库繁忙异常
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno); // 获取选定页面
            //先做日志
            byte[] log = Recover.insertLog(xid, pg, raw); // 生成插入操作的日志记录
            logger.log(log); // 记录日志
            //再执行插入
            short offset = PageX.insert(pg, raw); // 在页面中插入数据项
            pg.release(); // 释放页面资源
            return Types.addressToUid(pi.pgno, offset); // 返回插入数据项的唯一标识符

        } finally {
            // 将取出的pg重新插入pIndex
            if (pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg)); // 如果页面存在，则将其添加回页面索引并更新空闲空间大小
            } else {
                pIndex.add(pi.pgno, freeSpace); // 如果页面不存在，则将空闲空间大小添加回页面索引
            }
        }
    }

    /**
     * 关闭数据管理器
     */
    @Override
    public void close() {
        super.close(); // 关闭基类的资源
        logger.close(); // 关闭日志记录器

        PageOne.setVcClose(pageOne); // 设置页面One为关闭状态
        pageOne.release(); // 释放页面One的资源
        pc.close(); // 关闭页面缓存
    }

    /**
     * 记录数据项的操作到事务日志
     *
     * @param xid 事务ID
     * @param di  DataItem对象
     */
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di); // 生成更新操作的日志记录
        logger.log(log); // 记录日志
    }

    /**
     * 释放数据项的资源
     *
     * @param di DataItem对象
     */
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid()); // 释放数据项
    }

    /**
     * 从缓存中获取DataItem对象
     *
     * @param uid 唯一标识符
     * @return DataItem对象
     * @throws Exception 如果获取过程中发生异常
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1)); // 获取偏移量,保留uid的低16位作为偏移量
        uid >>>= 32;//将高32位移出，得到低32位作为页面号
        int pgno = (int) (uid & ((1L << 32) - 1)); // 获取页面号
        Page pg = pc.getPage(pgno); // 获取页面
        return DataItem.parseDataItem(pg, offset, this); // 解析数据项
    }


    /**
     * 释放缓存中的DataItem对象
     *
     * @param di DataItem对象
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release(); // 释放数据项所在的页面
    }


    /**
     * 在创建文件时初始化PageOne
     */
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw()); // 创建新的页面
        assert pgno == 1; // 确保页面号为1
        try {
            pageOne = pc.getPage(pgno); // 获取页面One
        } catch (Exception e) {
            Panic.panic(e); // 出现异常时触发panic
        }
        pc.flushPage(pageOne); // 刷新页面One到持久性存储介质
    }

    /**
     * 在打开已有文件时读入PageOne，并验证其正确性
     *
     * @return 如果页面One的验证通过则返回true，否则返回false
     */
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1); // 获取页面One
        } catch (Exception e) {
            Panic.panic(e); // 出现异常时触发panic
        }
        return PageOne.checkVc(pageOne); // 验证页面One的正确性
    }

    /**
     * 填充页面索引
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber(); // 获取页面数量
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i); // 获取指定页面
            } catch (Exception e) {
                Panic.panic(e); // 出现异常时触发panic
            }
            // 将页面及其空闲空间大小添加到页面索引中
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release(); // 释放页面资源
        }
    }


}
