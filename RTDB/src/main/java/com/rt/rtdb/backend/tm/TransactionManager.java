package com.rt.rtdb.backend.tm;

import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/**
 * 事务管理器接口
 *
 * @author ryh
 * @version 1.0
 * @create 2023/7/5 8:11
 * @since 1.0
 **/
public interface TransactionManager {
    long begin();                       // 开启一个新事务

    void commit(long xid);              // 提交一个事务

    void abort(long xid);               // 取消一个事务

    boolean isActive(long xid);         // 查询一个事务的状态是否是正在进行的状态

    boolean isCommitted(long xid);      // 查询一个事务的状态是否是已提交

    boolean isAborted(long xid);        // 查询一个事务的状态是否是已取消

    void close();                       // 关闭TM

    /**
     * 创建事务管理器
     *
     * @param path 文件路径
     * @return 创建的事务管理器实例
     * @throws IOException 如果创建文件、读取文件或写入文件时发生IO异常
     */
    public static TransactionManagerImpl create(String path) throws IOException {
        // 创建文件对象
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        // 如果文件已存在，则抛出异常
        if (!f.createNewFile()) {
            Panic.panic(Error.FileExistsException);
        }
        // 如果文件不可读或不可写，则抛出异常
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            // 创建RandomAccessFile对象，以读写模式打开文件
            raf = new RandomAccessFile(f, "rw");
            // 获取文件的通道
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写一个空的XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        // 将文件通道的位置设置为0
        fc.position(0);
        // 将字节缓冲区的内容写入文件通道
        fc.write(buf);
        // 返回创建的事务管理器实例
        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 打开事务管理器。
     *
     * @param path 文件路径
     * @return 打开的事务管理器实例
     */
    public static TransactionManagerImpl open(String path) {
        // 创建文件对象
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        // 如果文件不存在，则抛出异常
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        // 如果文件不可读或不可写，则抛出异常
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            // 创建RandomAccessFile对象，以读写模式打开文件
            raf = new RandomAccessFile(f, "rw");
            // 获取文件的通道
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 返回打开的事务管理器实例
        return new TransactionManagerImpl(raf, fc);
    }


}

