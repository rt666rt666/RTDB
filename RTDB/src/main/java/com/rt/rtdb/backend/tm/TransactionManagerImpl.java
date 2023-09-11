package com.rt.rtdb.backend.tm;

import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rt.rtdb.common.Error;

/**
 * 事务管理器实现类
 *
 * @author ryh
 * @version 1.0
 * @create 2023/7/5 8:15
 * @since 1.0
 **/
public class TransactionManagerImpl implements TransactionManager {

    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，永远为committed状态
    public static final long SUPER_XID = 0;
    // XID 文件后缀
    static final String XID_SUFFIX = ".xid";
    private RandomAccessFile file; // 用于操作文件的RandomAccessFile对象
    private FileChannel fc; // 文件通道对象，用于读写文件
    private long xidCounter; // 事务标识符计数器的值
    private Lock counterLock; // 用于对事务标识符计数器进行加锁操作的Lock对象

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter(); // 检查XID文件是否合法
    }

    /**
     * 检查 XID文件是否合法
     * 读取 XID_FILE_HEADER中的 xidcounter，根据它计算文件的理论长度，对比实际长度
     *
     * @author ryh
     * @date 2023/7/5 8:33
     */
    private void checkXIDCounter() {
        // 获取文件的长度
        long fileLen = 0;
        try {
            fileLen = file.length();//实际文件长度
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException); // 抛出BadXIDFileException异常
        }
        // 如果实际文件长度小于LEN_XID_HEADER_LENGTH，抛出BadXIDFileException异常
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }
        // 创建一个指定容量的ByteBuffer对象，用于存储从文件通道中读取的数据
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            // 将文件通道的位置设置为0，即从文件的开头开始读取数据
            fc.position(0);
            // 从文件通道读取数据到ByteBuffer中
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e); // 如果读取文件数据时发生异常，则抛出异常
        }
        // 将ByteBuffer中的数据解析为长整型数值，并赋值给xidCounter变量
        this.xidCounter = Parser.parseLong(buf.array());
        // 计算下一个事务标识符的位置
        long end = getXidPosition(this.xidCounter + 1);
        // 如果下一个事务标识符的位置与文件长度不匹配，抛出BadXIDFileException异常
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }


    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    //开始一个事务，并返回XID
    @Override
    public long begin() {
        counterLock.lock(); // 对计数器加锁，确保在多线程环境下的安全访问
        try {
            long xid = xidCounter + 1; // 计算下一个事务标识符
            updateXID(xid, FIELD_TRAN_ACTIVE); // 更新事务标识符的状态为活动状态
            incrXIDCounter(); // 递增事务标识符计数器的值
            return xid; // 返回事务标识符
        } finally {
            counterLock.unlock(); // 释放锁
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        // 事务标识符计数器自增
        xidCounter++;
        // 将自增后的计数器转换为字节数组，并将其包装成ByteBuffer对象
        ByteBuffer byteBuffer = ByteBuffer.wrap(Parser.long2Byte(xidCounter));

        try {
            // 设置文件通道的位置为0，即文件的开头
            fc.position(0);
            // 将ByteBuffer中的数据写入文件通道，实现更新事务标识符计数器的操作
            fc.write(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制将文件通道的数据刷新到磁盘，参数为false表示不强制刷新元数据，如最后修改时间等
            fc.force(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    // 更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        // 获取事务标识符在文件中的偏移量
        long offset = getXidPosition(xid);
        // 创建一个指定大小的字节数组
        byte[] temp = new byte[XID_FIELD_SIZE];
        // 将状态值存储到字节数组的第一个位置
        temp[0] = status;
        // 将temp字节数组包装成ByteBuffer对象，用于写入文件通道
        ByteBuffer byteBuffer = ByteBuffer.wrap(temp);

        try {
            // 设置文件通道的位置为事务标识符的偏移量
            fc.position(offset);
            // 将ByteBuffer中的数据写入文件通道
            fc.write(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 强制将文件通道的数据刷新到磁盘，参数为false表示不强制刷新元数据
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 检测指定XID的事务是否处于指定状态。
     *
     * @param xid    事务标识符
     * @param status 状态值
     * @return 如果事务处于指定状态，则返回true；否则返回false
     */
    private boolean checkXID(long xid, byte status) {
        // 根据XID计算偏移量
        long offset = getXidPosition(xid);
        // 创建一个指定容量的ByteBuffer对象，用于存储从文件通道中读取的数据
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            // 将文件通道的位置设置为偏移量，即从指定位置开始读取数据
            fc.position(offset);
            // 从文件通道读取数据到ByteBuffer中
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e); // 如果读取文件数据时发生异常，则抛出异常
        }
        // 检查ByteBuffer中的第一个字节是否等于指定状态值
        return buf.get(0) == status;
    }


    // 提交XID事务
    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
