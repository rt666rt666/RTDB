package com.rt.rtdb.backend.dm.pageCache;

import com.rt.rtdb.backend.common.AbstractCache;
import com.rt.rtdb.backend.dm.page.Page;
import com.rt.rtdb.backend.dm.page.PageImpl;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author RT666
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10; // 内存最小限制
    public static final String DB_SUFFIX = ".db"; // 数据库文件后缀

    private RandomAccessFile file; // 随机访问文件,该类的实例支持对随机访问文件的读写
    private FileChannel fc; // 文件通道
    private Lock fileLock; // 文件锁

    private AtomicInteger pageNumbers; // 页面编号计数器

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        //maxResource 缓存的最大缓存资源数
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException); // 内存太小异常
        }
        long length = 0;
        try {
            length = file.length(); // 获取文件长度
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock(); // 创建文件锁
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE); // 计算页面编号,PAGE_SIZE为8192
    }

    /**
     * 创建一个新页面，并使用指定的初始化数据初始化页面内容
     * @param initData 初始化数据
     * @return 新页面的编号
     */
    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet(); // incrementAndGet() 对整数进行自增操作
        Page pg = new PageImpl(pgno, initData, null); // 创建新页面对象
        flush(pg); // 将页面刷新到数据库文件
        return pgno;
    }

    /**
     * 根据页号从缓存中获取页面
     * @param pgno 页号
     * @return 页面对象
     * @throws Exception 如果获取页面出错
     */
    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long) pgno); // 从缓存中获取页面
    }

    /**
     * 根据页号从数据库文件中读取页数据，并包裹成Page
     * @param key 页号
     * @return 页面对象
     * @throws Exception 如果获取页面出错
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = PageCacheImpl.pageOffset(pgno); // 计算页的偏移量

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE); // 分配PAGE_SIZE大小的缓冲区
        fileLock.lock(); // 获取文件锁
        try {
            fc.position(offset); // 设置通道位置
            fc.read(buf); // 从通道读取数据到缓冲区
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock(); // 释放文件锁
        }
        return new PageImpl(pgno, buf.array(), this); // 创建包含读取数据的页面对象
    }

    /**
     * 释放缓存中的页面
     * @param pg 要释放的页面
     */
    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {// 如果页面被修改过，则将其刷新到数据库文件
            flush(pg);// 将修改过的页面刷新到数据库文件，将最新的数据持久化到磁盘
            pg.setDirty(false); // 将页面标记为未修改状态，以便在需要时可以避免重复的写入操作
        }
    }

    /**
     * 释放页面
     * @param page 要释放的页面
     */
    @Override
    public void release(Page page) {
        release((long) page.getPageNumber()); // 释放页面
    }

    /**
     * 刷新页面到数据库文件
     * @param pg 要刷新的页面
     */
    @Override
    public void flushPage(Page pg) {
        flush(pg); // 刷新页面到数据库文件
    }

    /**
     * 用于实际执行将页面刷新到数据库文件的操作
     * @param pg 要刷新的页面
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber(); // 获取页面编号
        long offset = pageOffset(pgno); // 计算页的偏移量

        fileLock.lock(); // 获取文件锁
        try {
            //ByteBuffer.wrap(byte[] array) 接受一个字节数组 array 作为参数，并返回一个新的 ByteBuffer 对象，该对象将给定的字节数组包装到缓冲区中
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset); // 设置通道位置
            fc.write(buf); // 将缓冲区数据写入通道
            fc.force(false); // 强制将数据刷新到磁盘，但不刷新元数据,如文件的最后修改时间
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock(); // 释放文件锁
        }
    }

    /**
     * 根据最大页号进行数据库文件的截断
     * @param maxPgno 最大页号
     */
    @Override
    public void truncateByBgno(int maxPgno) {
        //页面编号是从 1 开始的，所以要考虑下一个页面的编号
        long size = pageOffset(maxPgno + 1); // 计算截断后的文件大小
        try {
            file.setLength(size); // 设置文件长度
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno); // 设置页面编号计数器的值
    }

    /**
     * 关闭页面缓存，释放资源
     */
    @Override
    public void close() {
        super.close();
        try {
            fc.close(); // 关闭文件通道
            file.close(); // 关闭文件
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 获取当前页面编号
     * @return 当前页面编号
     */
    @Override
    public int getPageNumber() {
        return pageNumbers.intValue(); // 获取页面编号计数器的值，将 AtomicInteger 类型的 pageNumbers 转换为 int 类型的值
    }

    /**
     * 根据页号计算偏移量
     * @param pgno 页号
     * @return 偏移量
     */
    private static long pageOffset(int pgno) {
        //pgno - 1 表示给定页面编号 pgno 的前一个页面编号。因为页面编号是从 1 开始的，所以要考虑前一个页面的编号
        return (pgno - 1) * PAGE_SIZE; // 计算页的偏移量
    }

}
