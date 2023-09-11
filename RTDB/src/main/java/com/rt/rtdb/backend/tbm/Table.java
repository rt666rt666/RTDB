package com.rt.rtdb.backend.tbm;

import com.google.common.primitives.Bytes;
import com.rt.rtdb.backend.parser.statement.*;
import com.rt.rtdb.backend.tm.TransactionManagerImpl;
import com.rt.rtdb.backend.tbm.Field.ParseValueRes;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.backend.utils.ParseStringRes;
import com.rt.rtdb.backend.utils.Parser;
import com.rt.rtdb.common.Error;

import java.util.*;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 * @author RT666
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    long nextUid;
    List<Field> fields = new ArrayList<>();

    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 创建表格。
     *
     * @param tbm TableManager 对象
     * @param nextUid 下一个 UID
     * @param xid 事务 ID
     * @param create CreateTable 对象
     * @return 创建的 Table 对象
     * @throws Exception 如果创建过程中发生异常
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        // 创建 Table 对象，并设置表格名称和下一个 UID
        Table tb = new Table(tbm, create.tableName, nextUid);

        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;

            for(int j = 0; j < create.index.length; j ++) {
                // 判断字段是否需要创建索引
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }

            // 创建 Field 对象，并将其添加到 Table 的 fields 列表中
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        // 将 Table 对象持久化，并返回
        return tb.persistSelf(xid);
    }

    /**
     *
     * @param tbm TableManager 对象
     * @param uid 表格 UID
     */
    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    /**
     * 表格构造方法。
     *
     * @param tbm TableManager 对象
     * @param tableName 表格名称
     * @param nextUid 下一个 UID
     */
    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 解析自身。
     *
     * @param raw 原始字节数组
     * @return 解析后的 Table 对象
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        // 解析表格名称，并更新位置
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        // 解析下一个 UID，并更新位置
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        while(position < raw.length) {
            // 解析字段 UID，并更新位置
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            // 加载字段，并添加到 fields 列表中
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    /**
     * 持久化当前表格实例，并返回自身。
     *
     * @param xid 事务ID
     * @return 当前表格实例
     * @throws Exception 异常情况
     */
    private Table persistSelf(long xid) throws Exception {
        // 将名称、下一个UID和字段UID转换为字节数组
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        // 插入经过拼接的字节数组并更新表格UID
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * 根据条件从表格中删除数据，并返回删除的行数。
     *
     * @param xid 事务ID
     * @param delete 删除操作对象
     * @return 删除的行数
     * @throws Exception 异常情况
     */
    public int delete(long xid, Delete delete) throws Exception {
        // 解析删除操作的条件，获取要删除的UID列表
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        // 遍历UID列表，逐个删除对应的行
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 根据条件从表格中更新数据，并返回更新的行数。
     *
     * @param xid 事务ID
     * @param update 更新操作对象
     * @return 更新的行数
     * @throws Exception 异常情况
     */
    public int update(long xid, Update update) throws Exception {
        // 解析更新操作的条件，获取要更新的UID列表
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        // 根据字段名在fields列表中查找对应的字段
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        // 如果找不到对应的字段，则抛出FieldNotFoundException异常
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        // 将更新的值转换为对应字段类型的对象
        Object value = fd.string2Value(update.value);
        int count = 0;
        // 遍历UID列表，逐个更新对应的行
        for (Long uid : uids) {
            // 读取当前行的字节数组
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) {
                continue;
            }
            // 删除当前行
            ((TableManagerImpl)tbm).vm.delete(xid, uid);
            // 解析当前行的键值对，并更新指定字段的值
            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            // 将更新后的键值对转换为字节数组，并插入新的行
            raw = entry2Raw(entry);
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);

            count++;
            // 更新索引字段
            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 根据条件从表格中读取数据，并返回查询结果。
     *
     * @param xid 事务ID
     * @param read 查询操作对象
     * @return 查询结果字符串
     * @throws Exception 异常情况
     */
    public String read(long xid, Select read) throws Exception {
        // 解析查询操作的条件，获取要查询的UID列表
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        // 遍历UID列表，逐个读取对应的行，并将结果拼接为字符串
        for (Long uid : uids) {
            // 读取当前行的字节数组
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) {
                continue;
            }
            // 解析当前行的键值对，并将结果添加到StringBuilder中
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 将数据插入到表格中。
     *
     * @param xid 事务ID
     * @param insert 插入操作对象
     * @throws Exception 异常情况
     */
    public void insert(long xid, Insert insert) throws Exception {
        // 将插入的值转换为键值对
        Map<String, Object> entry = string2Entry(insert.values);
        // 将键值对转换为字节数组，并插入新的行
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        // 更新索引字段
        for (Field field : fields) {
            if(field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }
    /**
     * 将字符串数组转换为键值对。
     *
     * @param values 字符串数组，包含要转换的值
     * @return 转换后的键值对
     * @throws Exception 如果传入的值的个数与字段数量不一致，抛出InvalidValuesException异常
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        // 检查值的个数是否与字段数量一致
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException; // 抛出InvalidValuesException异常
        }

        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /**
     * 解析查询条件，并返回匹配的UID列表。
     *
     * @param where 查询条件对象
     * @return 匹配的UID列表
     * @throws Exception 如果查询条件中的字段没有索引，抛出FieldNotIndexedException异常；
     *                   如果查询条件中的字段在表格中不存在，抛出FieldNotFoundException异常
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single = false;
        Field fd = null;
        if(where == null) {
            // 如果查询条件为空，则使用第一个有索引的字段作为查询条件
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            // 根据查询条件找到对应的字段
            for (Field field : fields) {
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {
                        throw Error.FieldNotIndexedException; // 抛出FieldNotIndexedException异常
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) {
                throw Error.FieldNotFoundException; // 抛出FieldNotFoundException异常
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        // 根据查询条件进行搜索，并返回匹配的UID列表
        List<Long> uids = fd.search(l0, r0);
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    /**
     * 用于保存查询条件的计算结果。
     */
    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    /**
     * 计算查询条件
     * @param fd Field 对象
     * @param where 查询条件
     * @return CalWhereRes 对象，包含计算结果
     * @throws Exception 异常
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1); // 计算第一个表达式的结果
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2); // 计算第二个表达式的结果
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1); // 计算第一个表达式的结果
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2); // 计算第二个表达式的结果
                res.l1 = r.left; res.r1 = r.right;
                if(res.l1 > res.l0) { // 更新左值
                    res.l0 = res.l1;
                }
                if(res.r1 < res.r0) { // 更新右值
                    res.r0 = res.r1;
                }
                break;
            default:
                throw Error.InvalidLogOpException; // 抛出非法逻辑运算符异常
        }
        return res;
    }

    /**
     * 将包含键值对的 Map 对象转换为字符串形式的条目
     * @param entry 包含键值对的 Map 对象
     * @return 字符串形式的条目
     */
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size()-1) {
                sb.append("]"); // 最后一个键值对后添加 "]"
            } else {
                sb.append(", "); // 每个键值对之间添加 ", "
            }
        }
        return sb.toString();
    }

    /**
     * 将字节数组解析为包含键值对的 Map 对象
     * @param raw 字节数组
     * @return 包含键值对的 Map 对象
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length)); // 解析字节数组中对应字段的值
            entry.put(field.fieldName, r.v); // 将解析后的值放入 Map 中
            pos += r.shift; // 更新下一个字段的位置
        }
        return entry;
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
