package com.rt.rtdb.backend.tbm;

import com.google.common.primitives.Bytes;
import com.rt.rtdb.backend.im.BPlusTree;
import com.rt.rtdb.backend.parser.statement.SingleExpression;
import com.rt.rtdb.backend.tm.TransactionManagerImpl;
import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.backend.utils.ParseStringRes;
import com.rt.rtdb.backend.utils.Parser;
import com.rt.rtdb.common.Error;

import java.util.Arrays;
import java.util.List;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 * @author RT666
 */
public class Field {
    long uid; // 字段的唯一标识符
    private Table tb; // 所属的表
    String fieldName; // 字段名
    String fieldType; // 字段类型
    private long index; // 索引
    private BPlusTree bt; // B+树

    /**
     * 加载指定表和uid对应的字段，并返回Field对象。
     * @param tb 表对象
     * @param uid 字段的唯一标识符
     * @return Field对象
     */
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid); // 通过表管理器从虚拟内存中读取字段的原始数据
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null; // 确保字节数组不为空
        return new Field(uid, tb).parseSelf(raw); // 创建并解析Field对象，然后返回
    }

    /**
     * 使用指定的uid和表对象创建Field对象。
     * @param uid 字段的唯一标识符
     * @param tb 表对象
     */
    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    /**
     * 使用指定的表、字段名、字段类型和索引创建Field对象。
     * @param tb 表对象
     * @param fieldName 字段名
     * @param fieldType 字段类型
     * @param index 索引
     */
    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /**
     * 解析给定的字节数组，设置字段的属性，并返回Field对象自身。
     * @param raw 字节数组
     * @return Field对象自身
     */
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw); // 解析字节数组中的字符串数据
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length)); // 解析剩余部分的字符串数据
        fieldType = res.str;
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8)); // 解析剩余部分的长整型数据
        if(index != 0) { // 如果索引不为0，则加载对应的B+树
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * 使用指定的表、事务ID、字段名、字段类型和是否索引创建Field对象，并持久化到虚拟内存中。
     * @param tb 表对象
     * @param xid 事务ID
     * @param fieldName 字段名
     * @param fieldType 字段类型
     * @param indexed 是否索引
     * @return Field对象
     * @throws Exception 如果字段类型无效，则抛出异常
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType); // 检查字段类型是否有效
        Field f = new Field(tb, fieldName, fieldType, 0); // 创建Field对象(index为0代表没有索引)
        if(indexed) {
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm); // 创建一个新的B+树
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm); // 加载B+树
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid); // 持久化Field对象到虚拟内存中
        return f;
    }

    /**
     * 将Field对象持久化到虚拟内存中。
     * @param xid 事务ID
     * @throws Exception 如果发生错误，则抛出异常
     */
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName); // 将字段名转换为字节数组
        byte[] typeRaw = Parser.string2Byte(fieldType); // 将字段类型转换为字节数组
        byte[] indexRaw = Parser.long2Byte(index); // 将索引转换为字节数组
        // 将字段的原始数据插入虚拟内存，并获得唯一标识符
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    /**
     * 检查字段类型是否有效，如果无效则抛出异常。
     * @param fieldType 字段类型
     * @throws Exception 如果字段类型无效，则抛出异常
     */
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException; // 如果字段类型无效，则抛出异常
        }
    }

    /**
     * 判断字段是否有索引。
     * @return 如果有索引，则返回true；否则返回false
     */
    public boolean isIndexed() {
        return index != 0;
    }

    /**
     * 将键值对插入到B+树中。
     * @param key 键
     * @param uid 值的唯一标识符
     * @throws Exception 如果发生错误，则抛出异常
     */
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key); // 将键转换为唯一标识符
        bt.insert(uKey, uid); // 将键值对插入B+树中
    }

    /**
     * 在B+树中搜索位于指定范围的值，并返回结果列表。
     * @param left 范围左边界
     * @param right 范围右边界
     * @return 结果列表
     * @throws Exception 如果发生错误，则抛出异常
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right); // 在B+树中搜索位于指定范围的值，并返回结果列表
    }
    /**
     * 将字符串转换为相应的值
     *
     * @param str 字符串值
     * @return 转换后的值
     */
    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str); // 将字符串解析为int值
            case "int64":
                return Long.parseLong(str); // 将字符串解析为long值
            case "string":
                return str; // 字符串类型直接返回原字符串
        }
        return null;
    }

    /**
     * 将值转换为唯一标识符
     *
     * @param key 值
     * @return 唯一标识符
     */
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key); // 将字符串转换为唯一标识符
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint; // 将int值作为唯一标识符
            case "int64":
                uid = (long)key; // 将long值作为唯一标识符
                break;
        }
        return uid;
    }

    /**
     * 将值转换为字节数组
     *
     * @param v 值
     * @return 字节数组
     */
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v); // 将int值转换为字节数组
                break;
            case "int64":
                raw = Parser.long2Byte((long)v); // 将long值转换为字节数组
                break;
            case "string":
                raw = Parser.string2Byte((String)v); // 将字符串转换为字节数组
                break;
        }
        return raw;
    }

    /**
     * 解析字节数组为值和偏移量
     */
    class ParseValueRes {
        Object v; // 值
        int shift; // 偏移量
    }

    /**
     * 解析字节数组为值和偏移量
     *
     * @param raw 字节数组
     * @return 包含值和偏移量的解析结果
     */
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4)); // 解析字节数组为int值
                res.shift = 4; // 设置偏移量为4
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8)); // 解析字节数组为long值
                res.shift = 8; // 设置偏移量为8
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str; // 解析字节数组为字符串
                res.shift = r.next; // 设置偏移量为下一个位置
                break;
        }
        return res;
    }

    /**
     * 将值转换为字符串表示形式
     *
     * @param v 值
     * @return 字符串表示形式
     */
    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v); // 将int值转换为字符串表示形式
                break;
            case "int64":
                str = String.valueOf((long)v); // 将long值转换为字符串表示形式
                break;
            case "string":
                str = (String)v; // 字符串类型直接返回原字符串
                break;
        }
        return str;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
            .append(fieldName)
            .append(", ")
            .append(fieldType)
            .append(index!=0?", Index":", NoIndex")
            .append(")")
            .toString();
    }
    /**
     * 根据传入的表达式计算字段值
     *
     * @param exp 单个表达式
     * @return 计算结果
     * @throws Exception 如果出现异常
     */
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null; // 初始化变量v为null
        FieldCalRes res = new FieldCalRes(); // 创建FieldCalRes对象res
        switch(exp.compareOp) { // 根据表达式比较运算符执行不同的操作
            case "<":
                res.left = 0; // 设置res的left为0
                v = string2Value(exp.value); // 调用string2Value方法将exp的value转换为对应类型的值并赋给v
                res.right = value2Uid(v); // 调用value2Uid方法将v转换为唯一标识符并赋给res的right
                if(res.right > 0) {
                    res.right --; // 如果right大于0，则将right减1
                }
                break;
            case "=":
                v = string2Value(exp.value); // 调用string2Value方法将exp的value转换为对应类型的值并赋给v
                res.left = value2Uid(v); // 调用value2Uid方法将v转换为唯一标识符并赋给res的left
                res.right = res.left; // 将left的值赋给right
                break;
            case ">":
                res.right = Long.MAX_VALUE; // 设置res的right为Long的最大值
                v = string2Value(exp.value); // 调用string2Value方法将exp的value转换为对应类型的值并赋给v
                res.left = value2Uid(v) + 1; // 将v转换为唯一标识符并加1赋给res的left
                break;
        }
        return res; // 返回计算结果res
    }

}
