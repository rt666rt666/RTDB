package com.rt.rtdb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.backend.utils.Parser;
import com.rt.rtdb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 日志文件读写
 *
 * 日志文件标准格式为：
 *
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 *
 * XChecksum 为后续所有日志计算的Checksum，int类型
 *
 * 每条正确日志的格式为：
 *
 * [Size] [Checksum] [Data]
 *
 * Size 4字节int 标识Data长度
 *
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file; // 日志文件
    private FileChannel fc; // 文件通道
    private Lock lock; // 文件锁

    private long position; // 当前日志指针的位置
    private long fileSize; // 初始化时记录，log操作不更新
    private int xChecksum; // 所有日志计算的校验和

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init() {
        // 获取日志文件的大小并检查是否小于4字节
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        // 读取XChecksum
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //array()获取 ByteBuffer 对象所包含的字节数据的底层字节数组
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        // 检查并移除bad tail
        checkAndRemoveTail();
    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        // 将文件通道的位置重置为起始位置
        rewind();

        int xCheck = 0;
        while (true) {
            // 获取下一个日志条目
            byte[] log = internNext();
            if (log == null) {
                break;
            }
            // 计算校验和
            xCheck = calChecksum(xCheck, log);
        }
        // 检查校验和是否匹配
        if (xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }
        try {
            // 截断文件，移除损坏的尾部数据
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 设置文件指针位置为截断后的位置
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 将文件通道的位置重置为起始位置
        rewind();
    }

    /**
     * 计算校验和
     *
     * @param xCheck 初始校验和值
     * @param log 日志数据
     * @return 数据的校验和
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 记录日志
     * @param data
     */
    @Override
    public void log(byte[] data) {
        // 封装日志数据
        byte[] log = wrapLog(data);
        // 将字节数组包装成 ByteBuffer
        ByteBuffer buf = ByteBuffer.wrap(log);
        // 获取文件通道的锁
        lock.lock();
        try {
            // 将文件指针设置到文件末尾
            fc.position(fc.size());
            // 将数据写入文件通道
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            // 释放文件通道的锁
            lock.unlock();
        }
        // 更新校验和
        updateXChecksum(log);
    }


    /**
     * 更新总的校验和
     *
     * @param log 新写入的日志数据
     */
    private void updateXChecksum(byte[] log) {
        // 计算新的校验和
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            // 将文件指针设置到文件开头
            fc.position(0);
            // 将新的校验和写入文件
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            // 强制将数据刷新到磁盘
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 封装日志数据
     *
     * @param data 日志数据
     * @return 封装后的日志
     */
    private byte[] wrapLog(byte[] data) {
        // 计算校验和
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        // 获取数据的长度
        byte[] size = Parser.int2Byte(data.length);
        // 将长度、校验和和数据连接起来组成新的字节数组
        return Bytes.concat(size, checksum, data);
    }

    // 截断文件
    @Override
    public void truncate(long x) throws Exception {
        // 获取文件通道的锁
        lock.lock();
        try {
            // 调用文件通道的截断方法
            fc.truncate(x);
        } finally {
            // 释放文件通道的锁
            lock.unlock();
        }
    }


    /**
     * 读取并解析下一条日志
     *
     * @return 解析成功的日志数据，如果解析失败或达到文件末尾返回null
     */
    private byte[] internNext() {
        // 检查是否超过文件大小
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            // 设置文件通道的位置为当前位置
            fc.position(position);
            // 读取4个字节到tmp缓冲区
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        // 解析tmp缓冲区中的字节数组为整数值，获取大小
        int size = Parser.parseInt(tmp.array());
        // 检查是否超过文件大小
        if (position + size + OF_DATA > fileSize) {
            return null;
        }
        // 分配一个大小为 OF_DATA + size 的ByteBuffer缓冲区
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            // 设置文件通道的位置为当前位置
            fc.position(position);
            // 读取数据到buf缓冲区
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        // 获取buf缓冲区的字节数组
        byte[] log = buf.array();
        // 计算数据部分的校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 解析校验和字段的字节数组为整数值
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        // 检查校验和是否匹配
        if (checkSum1 != checkSum2) {
            return null;
        }
        // 更新位置
        position += log.length;
        return log;
    }

    @Override
    // 获取下一个日志
    public byte[] next() {
        // 获取文件通道的锁
        lock.lock();
        try {
            // 调用 internNext() 方法获取下一个日志
            byte[] log = internNext();
            // 如果日志为空，则返回 null
            if (log == null) return null;
            // 截取日志中的数据部分（从 OF_DATA 开始）
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            // 释放文件通道的锁
            lock.unlock();
        }
    }

    /**
     * 倒回文件指针
     */
    @Override
    public void rewind() {
        // 将位置设置为 4
        position = 4;
    }


    @Override
    // 关闭文件和文件通道
    public void close() {
        try {
            // 关闭文件通道
            fc.close();
            // 关闭文件
            file.close();
        } catch (IOException e) {
            // 发生异常时调用 Panic.panic() 方法进行处理
            Panic.panic(e);
        }
    }

}
