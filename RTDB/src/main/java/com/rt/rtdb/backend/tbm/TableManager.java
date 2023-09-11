package com.rt.rtdb.backend.tbm;

import com.rt.rtdb.backend.dm.DataManager;
import com.rt.rtdb.backend.parser.statement.*;
import com.rt.rtdb.backend.utils.Parser;
import com.rt.rtdb.backend.vm.VersionManager;

/**
 * TableManager接口用于管理表的操作。
 * @author RT666
 */
public interface TableManager {
    /**
     * 开始一个事务，并返回事务的结果。
     * @param begin 开始操作的参数对象
     * @return begin操作的响应结果
     */
    BeginRes begin(Begin begin);

    /**
     * 提交指定事务，并返回提交操作的结果。
     * @param xid 事务ID
     * @return commit操作的响应结果
     * @throws Exception 抛出异常
     */
    byte[] commit(long xid) throws Exception;

    /**
     * 终止指定事务，并返回终止操作的结果。
     * @param xid 事务ID
     * @return abort操作的响应结果
     */
    byte[] abort(long xid);

    /**
     * 显示指定事务或所有事务的表信息，并返回显示操作的结果。
     * @param xid 事务ID
     * @return show操作的响应结果
     */
    byte[] show(long xid);

    /**
     * 在指定事务中创建表，并返回创建操作的结果。
     * @param xid 事务ID
     * @param create 创建表的参数对象
     * @return create操作的响应结果
     * @throws Exception 抛出异常
     */
    byte[] create(long xid, Create create) throws Exception;

    /**
     * 在指定事务中插入数据到表中，并返回插入操作的结果。
     * @param xid 事务ID
     * @param insert 插入数据的参数对象
     * @return insert操作的响应结果
     * @throws Exception 抛出异常
     */
    byte[] insert(long xid, Insert insert) throws Exception;

    /**
     * 在指定事务中从表中读取数据，并返回读取操作的结果。
     * @param xid 事务ID
     * @param select 读取数据的参数对象
     * @return read操作的响应结果
     * @throws Exception 抛出异常
     */
    byte[] read(long xid, Select select) throws Exception;

    /**
     * 在指定事务中更新表中的数据，并返回更新操作的结果。
     * @param xid 事务ID
     * @param update 更新数据的参数对象
     * @return update操作的响应结果
     * @throws Exception 抛出异常
     */
    byte[] update(long xid, Update update) throws Exception;

    /**
     * 在指定事务中删除表中的数据，并返回删除操作的结果。
     * @param xid 事务ID
     * @param delete 删除数据的参数对象
     * @return delete操作的响应结果
     * @throws Exception 抛出异常
     */
    byte[] delete(long xid, Delete delete) throws Exception;

    /**
     * 创建TableManager实例，并返回实例对象。
     * @param path 表管理器的路径
     * @param vm 版本管理器
     * @param dm 数据管理器
     * @return TableManager实例
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    /**
     * 打开TableManager实例，并返回实例对象。
     * @param path 表管理器的路径
     * @param vm 版本管理器
     * @param dm 数据管理器
     * @return TableManager实例
     */
    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
