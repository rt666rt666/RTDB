package com.rt.rtdb.backend.dm.logger;

import com.rt.rtdb.common.Error;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.backend.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    void log(byte[] data);  // 记录日志

    void truncate(long x) throws Exception;  // 截断日志

    byte[] next();  // 获取下一个日志

    void rewind();  // 回到日志的起始位置

    void close();  // 关闭日志

    /**
     * 创建一个新的日志文件并返回Logger实例
     *
     * @param path 日志文件路径
     * @return Logger实例
     */
    public static Logger create(String path) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            //如果createNewFile()方法返回true（文件创建成功），则取反为false
            // 如果createNewFile()方法返回false（文件创建失败），则取反为true，表示文件已存在
            if (!f.createNewFile()) {  // 检查文件是否已经存在
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {  // 如果文件不可读或不可写，则抛出异常
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

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));  // 创建一个包含初始值的ByteBuffer
        try {
            fc.position(0);
            fc.write(buf);  // 将初始值写入文件
            fc.force(false);  // 强制将数据刷入磁盘
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);  // 返回LoggerImpl实例
    }

    /**
     * 打开现有的日志文件并返回Logger实例
     *
     * @param path 日志文件路径
     * @return Logger实例
     */
    public static Logger open(String path) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        if (!f.exists()) {  // 如果文件不存在，则抛出异常
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {  // 如果文件不可读或不可写，则抛出异常
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

        LoggerImpl lg = new LoggerImpl(raf, fc);  // 创建LoggerImpl实例
        lg.init();  // 初始化LoggerImpl

        return lg;  // 返回LoggerImpl实例
    }
}
