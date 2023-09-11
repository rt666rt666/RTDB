package com.rt.rtdb.backend.dm.pageCache;

import com.rt.rtdb.backend.dm.page.Page;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author RT666
 */
public interface PageCache {

    // 页面大小为2^13，即8192,通常用于表示页面大小或缓冲区大小，以字节为单位
    public static final int PAGE_SIZE = 1 << 13;

    /**
     * 创建一个新页面，并使用指定的初始化数据初始化页面内容
     * @param initData 初始化数据
     * @return 新页面的编号
     */
    int newPage(byte[] initData);

    /**
     * 根据页面编号获取对应的页面对象
     * @param pgno 页面编号
     * @return 对应的页面对象
     * @throws Exception 如果获取页面失败抛出异常
     */
    Page getPage(int pgno) throws Exception;

    /**
     * 关闭PageCache，释放资源
     */
    void close();

    /**
     * 释放指定的页面，使其可被重用
     * @param page 需要释放的页面
     */
    void release(Page page);

    /**
     * 根据指定的最大页面编号截断页面缓存中的页面
     * @param maxPgno 最大页面编号
     */
    void truncateByBgno(int maxPgno);

    /**
     * 获取页面缓存中当前的页面数量
     * @return 当前的页面数量
     */
    int getPageNumber();

    /**
     * 将指定的页面刷新到磁盘上
     * @param pg 需要刷新的页面
     */
    void flushPage(Page pg);

    /**
     * 创建一个新的PageCacheImpl对象
     * @param path 文件路径
     * @param memory 内存大小
     * @return 新的PageCacheImpl对象
     */
    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    /**
     * 打开一个现有的PageCacheImpl对象
     * @param path 文件路径
     * @param memory 内存大小
     * @return 打开的PageCacheImpl对象
     */
    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}