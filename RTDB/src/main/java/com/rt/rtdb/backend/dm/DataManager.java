package com.rt.rtdb.backend.dm;

import com.rt.rtdb.backend.dm.dataItem.DataItem;
import com.rt.rtdb.backend.dm.logger.Logger;
import com.rt.rtdb.backend.dm.page.PageOne;
import com.rt.rtdb.backend.dm.pageCache.PageCache;
import com.rt.rtdb.backend.tm.TransactionManager;

/**
 * @author ryh
 * @date 2023/7/11
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * 创建DataManager实例
     * @param path 数据存储路径
     * @param mem 内存限制
     * @param tm 事务管理器
     * @return 创建的DataManager实例
     */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem); // 创建页面缓存
        Logger lg = Logger.create(path); // 创建日志记录器

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm); // 创建DataManagerImpl实例
        dm.initPageOne(); // 初始化页面One
        return dm; // 返回DataManager实例
    }

    /**
     * 打开已有的DataManager实例
     * @param path 数据存储路径
     * @param mem 内存限制
     * @param tm 事务管理器
     * @return 打开的DataManager实例
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem); // 打开页面缓存
        Logger lg = Logger.open(path); // 打开日志记录器
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm); // 创建DataManagerImpl实例
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc); // 如果页面One验证失败，执行数据恢复
        }
        dm.fillPageIndex(); // 填充页面索引
        PageOne.setVcOpen(dm.pageOne); // 设置页面One为打开状态
        dm.pc.flushPage(dm.pageOne); // 刷新页面One到持久性存储介质
        return dm; // 返回打开的DataManager实例
    }

}
