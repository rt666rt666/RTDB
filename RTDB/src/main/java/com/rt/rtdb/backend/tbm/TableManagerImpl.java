package com.rt.rtdb.backend.tbm;

import com.rt.rtdb.backend.dm.DataManager;
import com.rt.rtdb.backend.parser.statement.*;
import com.rt.rtdb.backend.utils.Parser;
import com.rt.rtdb.backend.vm.VersionManager;
import com.rt.rtdb.common.Error;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 表管理器的实现类
 * @author ryh
 * @version 1.0
 * @since 1.0
 * @create 2023/8/5 16:55
 **/
public class TableManagerImpl implements TableManager {
    VersionManager vm; // 版本管理器
    DataManager dm; // 数据管理器
    private Booter booter; // 启动器
    private Map<String, Table> tableCache; // 表缓存
    private Map<Long, List<Table>> xidTableCache; // 事务ID-表列表缓存
    private Lock lock; // 锁对象

    /**
     * 构造方法
     * @param vm 版本管理器
     * @param dm 数据管理器
     * @param booter 启动器
     */
    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm; // 初始化版本管理器
        this.dm = dm; // 初始化数据管理器
        this.booter = booter; // 初始化启动器
        this.tableCache = new HashMap<>(); // 创建表缓存
        this.xidTableCache = new HashMap<>(); // 创建事务ID-表列表缓存
        lock = new ReentrantLock(); // 创建可重入锁
        loadTables(); // 加载表数据
    }

    /**
     * 加载表数据
     */
    private void loadTables() {
        long uid = firstTableUid(); // 获取第一个表的UID
        while (uid != 0) { // 当UID不为0时，循环加载每个表
            Table tb = Table.loadTable(this, uid); // 加载表
            uid = tb.nextUid; // 更新UID为下一个表的UID
            tableCache.put(tb.name, tb); // 将表添加到表缓存中，以表名为键
        }
    }

    /**
     * 获取第一个表的UID
     * @return 第一个表的UID
     */
    private long firstTableUid() {
        byte[] raw = booter.load(); // 从启动器加载原始数据
        return Parser.parseLong(raw); // 将原始数据解析为长整型并返回
    }

    /**
     * 更新第一个表的UID
     * @param uid 要更新的UID
     */
    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid); // 将UID转换为字节数组
        booter.update(raw); // 更新启动器中的数据
    }

    /**
     * 开启新事务
     * @param begin 开始事务的语句
     * @return 开始事务的结果
     */
    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes(); // 创建BeginRes对象
        int level = begin.isRepeatableRead ? 1 : 0; // 根据传入的Begin对象确定事务隔离级别
        res.xid = vm.begin(level); // 调用版本管理器开始新事务，获取事务ID
        res.result = "begin".getBytes(); // 设置结果为"begin"
        return res; // 返回BeginRes对象
    }

    /**
     * 提交事务
     * @param xid 要提交的事务ID
     * @return 提交事务的结果
     * @throws Exception 提交事务时可能发生异常
     */
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid); // 提交指定的事务
        return "commit".getBytes(); // 返回包含"commit"的字节数组
    }

    /**
     * 终止事务
     * @param xid 要终止的事务ID
     * @return 终止事务的结果
     */
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid); // 终止指定的事务
        return "abort".getBytes(); // 返回包含"abort"的字节数组
    }

    /**
     * 显示表信息
     * @param xid 事务ID
     * @return 表的信息
     */
    @Override
    public byte[] show(long xid) {
        lock.lock(); // 获取锁
        try {
            StringBuilder sb = new StringBuilder(); // 创建StringBuilder对象
            for (Table tb : tableCache.values()) { // 遍历表缓存中的每个表
                sb.append(tb.toString()).append("\n"); // 将表的信息追加到StringBuilder中
            }
            List<Table> t = xidTableCache.get(xid); // 根据事务ID从事务ID-表列表缓存获取表列表
            if (t == null) { // 如果表列表为空，返回换行符的字节数组
                return "\n".getBytes();
            }
            for (Table tb : t) { // 遍历表列表中的每个表
                sb.append(tb.toString()).append("\n"); // 将表的信息追加到StringBuilder中
            }
            return sb.toString().getBytes(); // 返回包含表信息的字节数组
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 创建新表时，采用头插法，每次创建表都需要更新 Booter 文件
     * @param xid 事务ID
     * @param create 创建表的语句
     * @return 创建表的结果
     * @throws Exception 创建表时可能发生异常
     */
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock(); // 获取锁
        try {
            if (tableCache.containsKey(create.tableName)) { // 如果表缓存中已存在该表名
                throw Error.DuplicatedTableException; // 抛出重复表异常
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create); // 创建表，并返回Table对象
            updateFirstTableUid(table.uid); // 更新第一个表的UID
            tableCache.put(create.tableName, table); // 将新表添加到表缓存中，以表名为键
            if (!xidTableCache.containsKey(xid)) { // 如果事务ID-表列表缓存中不存在该事务ID
                xidTableCache.put(xid, new ArrayList<>()); // 在事务ID-表列表缓存中为该事务ID创建一个空的表列表
            }
            xidTableCache.get(xid).add(table); // 将新表添加到该事务ID对应的表列表中
            return ("create " + create.tableName).getBytes(); // 返回包含"create tableName"的字节数组
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 插入记录
     * @param xid 事务ID
     * @param insert 插入记录的语句
     * @return 插入记录的结果
     * @throws Exception 插入记录时可能发生异常
     */
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock(); // 获取锁，对共享资源进行保护
        Table table = tableCache.get(insert.tableName); // 从表缓存中根据表名获取Table对象
        lock.unlock(); // 释放锁
        if (table == null) { // 如果获取的Table对象为空
            throw Error.TableNotFoundException; // 抛出表未找到异常
        }
        table.insert(xid, insert); // 在Table对象上执行插入操作
        return "insert".getBytes(); // 返回包含"insert"的字节数组
    }

    /**
     * 读取记录
     * @param xid 事务ID
     * @param read 读取记录的语句
     * @return 读取的记录
     * @throws Exception 读取记录时可能发生异常
     */
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock(); // 获取锁
        Table table = tableCache.get(read.tableName); // 从表缓存中根据表名获取Table对象
        lock.unlock(); // 释放锁
        if (table == null) { // 如果获取的Table对象为空
            throw Error.TableNotFoundException; // 抛出表未找到异常
        }
        return table.read(xid, read).getBytes(); // 在Table对象上执行读取操作，并返回读取结果的字节数组
    }
    /**
     * 使用指定的事务ID和更新参数更新表。
     * @param xid 事务的唯一标识符。
     * @param update 要在表上执行的更新操作。
     * @return 包含字符串"update count"的字节数组，其中count是更新的记录数
     * @author ryh
     * @date 2023/8/5 16:53
     */
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock(); // 获取锁
        Table table = tableCache.get(update.tableName); // 从表缓存中根据表名获取Table对象
        lock.unlock(); // 释放锁
        if (table == null) { // 如果获取的Table对象为空
            throw Error.TableNotFoundException; // 抛出表未找到异常
        }
        int count = table.update(xid, update); // 在Table对象上执行更新操作，并获取更新的记录数
        return ("update " + count).getBytes(); // 返回包含"update count"的字节数组，其中count为更新的记录数
    }


    /**
     * 使用指定的事务ID和删除参数从表中删除记录。
     *  @param xid 事务的唯一标识符。
     *  @param delete 要在表上执行的删除操作。
     *  @return 包含字符串"delete count"的字节数组，其中count是删除的记录数
     * @date 2023/8/5 16:54
     */
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock(); // 获取锁
        Table table = tableCache.get(delete.tableName); // 从表缓存中根据表名获取Table对象
        lock.unlock(); // 释放锁
        if (table == null) { // 如果获取的Table对象为空
            throw Error.TableNotFoundException; // 抛出表未找到异常
        }
        int count = table.delete(xid, delete); // 在Table对象上执行删除操作，并获取删除的记录数
        return ("delete " + count).getBytes(); // 返回包含"delete count"的字节数组，其中count为删除的记录数
    }
}
