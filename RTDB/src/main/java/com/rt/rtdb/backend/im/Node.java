package com.rt.rtdb.backend.im;

import com.rt.rtdb.backend.common.SubArray;
import com.rt.rtdb.backend.dm.dataItem.DataItem;
import com.rt.rtdb.backend.tm.TransactionManagerImpl;
import com.rt.rtdb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0; // 叶子节点标志的偏移量
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1; // 键数量的偏移量
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2; // 兄弟节点的偏移量
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8; // 节点头部大小
    static final int BALANCE_NUMBER = 32; // 平衡数
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2); // 节点大小

    BPlusTree tree; // B+树实例
    DataItem dataItem; // 数据项
    SubArray raw; // 原始数据数组
    long uid; // 节点的唯一标识符

    /**
     * 设置原始数据数组的叶子节点标志
     * @param raw 原始数据数组
     * @param isLeaf 是否为叶子节点
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1; // 将原始数据数组的叶子节点标志设置为1
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0; // 将原始数据数组的叶子节点标志设置为0
        }
    }

    /**
     * 获取原始数据数组的叶子节点标志
     * @param raw 原始数据数组
     * @return 是否为叶子节点
     */
    static boolean getRawIfLeaf(SubArray raw) {
        // 返回原始数据数组的叶子节点标志是否为1
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    /**
     * 设置原始数据数组的键数量
     * @param raw 原始数据数组
     * @param noKeys 键数量
     */
    static void setRawNoKeys(SubArray raw, int noKeys) {
        // 将键数量转换为字节数组，并复制到原始数据数组中的相应位置
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    /**
     * 获取原始数据数组的键数量
     * @param raw 原始数据数组
     * @return 键数量
     */
    static int getRawNoKeys(SubArray raw) {
        // 将原始数据数组中的键数量字节数组转换为short类型，并返回其值
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    /**
     * 设置原始数据数组的兄弟节点标识符
     * @param raw 原始数据数组
     * @param sibling 兄弟节点标识符
     */
    static void setRawSibling(SubArray raw, long sibling) {
        // 将兄弟节点标识符转换为字节数组，并复制到原始数据数组中的相应位置
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    /**
     * 获取原始数据数组的兄弟节点标识符
     * @param raw 原始数据数组
     * @return 兄弟节点标识符
     */
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8)); // 将原始数据数组中的兄弟节点标识符字节数组转换为long类型，并返回其值
    }

    /**
     * 设置原始数据数组的第k个子节点标识符
     * @param raw 原始数据数组
     * @param uid 子节点标识符
     * @param kth 第k个子节点
     */
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2); // 计算第k个子节点标识符在原始数据数组中的偏移量
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8); // 将子节点标识符转换为字节数组，并复制到原始数据数组中的相应位置
    }

    /**
     * 获取原始数据数组的第k个子节点标识符
     * @param raw 原始数据数组
     * @param kth 第k个子节点
     * @return 子节点标识符
     */
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2); // 计算第k个子节点标识符在原始数据数组中的偏移量
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8)); // 将原始数据数组中的子节点标识符字节数组转换为long类型，并返回其值
    }

    /**
     * 设置原始数据数组中第k个键的值
     * @param raw 原始数据数组
     * @param key 键值
     * @param kth 第k个键
     */
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8; // 计算第k个键的值在原始数据数组中的偏移量
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8); // 将键值转换为字节数组，并复制到原始数据数组中的相应位置
    }

    /**
     * 获取原始数据数组中第k个键的值
     * @param raw 原始数据数组
     * @param kth 第k个键
     * @return 键值
     */
    static long getRawKthKey(SubArray raw, int kth) {
        // 计算第k个键的值在原始数据数组中的偏移量
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        // 将原始数据数组中的键值字节数组转换为long类型，并返回其值
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    /**
     * 将原始数据数组中第k个键及其后面的数据复制到另一个原始数据数组中
     * @param from 源原始数据数组
     * @param to 目标原始数据数组
     * @param kth 第k个键
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        // 计算第k个键及其后面的数据在源原始数据数组中的偏移量
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        // 将源原始数据数组中的第k个键及其后面的数据复制到目标原始数据数组中
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    /**
     * 将原始数据数组中第k个键及其后面的数据向后移动
     * @param raw 原始数据数组
     * @param kth 第k个键
     */
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2); // 计算起始位置
        int end = raw.start + NODE_SIZE - 1; // 计算结束位置
        for (int i = end; i >= begin; i--) { // 从结束位置向起始位置遍历
            raw.raw[i] = raw.raw[i - (8 * 2)]; // 向后移动数据
        }
    }

    /**
     * 创建新的根节点的原始数据数组
     * @param left 左子节点的UID
     * @param right 右子节点的UID
     * @param key 键值
     * @return 新的根节点的原始数据数组
     */
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE); // 创建新的原始数据数组

        setRawIsLeaf(raw, false); // 设置为非叶子节点
        setRawNoKeys(raw, 2); // 设置键的数量
        setRawSibling(raw, 0); // 设置兄弟节点的UID
        setRawKthSon(raw, left, 0); // 设置左子节点的UID
        setRawKthKey(raw, key, 0); // 设置键值
        setRawKthSon(raw, right, 1); // 设置右子节点的UID
        setRawKthKey(raw, Long.MAX_VALUE, 1); // 设置键值

        return raw.raw; // 返回新的原始数据数组
    }

    /**
     * 创建新的空根节点的原始数据数组
     * @return 新的空根节点的原始数据数组
     */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE); // 创建新的原始数据数组

        setRawIsLeaf(raw, true); // 设置为叶子节点
        setRawNoKeys(raw, 0); // 设置键的数量
        setRawSibling(raw, 0); // 设置兄弟节点的UID

        return raw.raw; // 返回新的原始数据数组
    }

    /**
     * 加载节点
     * @param bTree B+树对象
     * @param uid 节点的UID
     * @return 加载的节点对象
     * @throws Exception 异常
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid); // 从数据管理器中读取数据项
        assert di != null; // 断言数据项不为空
        Node n = new Node(); // 创建新的节点对象
        n.tree = bTree; // 设置节点所属的B+树对象
        n.dataItem = di; // 设置节点的数据项
        n.raw = di.data(); // 设置节点的原始数据数组
        n.uid = uid; // 设置节点的UID
        return n; // 返回加载的节点对象
    }

    /**
     * 释放节点
     */
    public void release() {
        dataItem.release(); // 释放节点的数据项
    }

    /**
     * 判断节点是否为叶子节点
     * @return 是否为叶子节点
     */
    public boolean isLeaf() {
        dataItem.rLock(); // 加读锁保证数据的一致性
        try {
            return getRawIfLeaf(raw); // 获取节点是否为叶子节点的标志位
        } finally {
            dataItem.rUnLock(); // 释放读锁
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 搜索下一个节点
     * @param key 键值
     * @return 搜索结果对象
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock(); // 加读锁保证数据的一致性
        try {
            SearchNextRes res = new SearchNextRes(); // 创建搜索结果对象
            int noKeys = getRawNoKeys(raw); // 获取节点的键数量
            for(int i = 0; i < noKeys; i ++) { // 遍历节点的键
                long ik = getRawKthKey(raw, i); // 获取第i个键的值
                if(key < ik) { // 如果目标键值小于第i个键值
                    res.uid = getRawKthSon(raw, i); // 设置搜索结果的UID为第i个子节点的UID
                    res.siblingUid = 0; // 设置搜索结果的兄弟节点UID为0
                    return res; // 返回搜索结果
                }
            }
            res.uid = 0; // 如果没有找到合适的子节点，设置搜索结果的UID为0
            res.siblingUid = getRawSibling(raw); // 设置搜索结果的兄弟节点UID为节点的兄弟节点UID
            return res; // 返回搜索结果

        } finally {
            dataItem.rUnLock(); // 释放读锁
        }
    }

    /**
     * 叶子节点搜索范围结果类
     */
    class LeafSearchRangeRes {
        List<Long> uids; // 叶子节点搜索范围内的UID列表
        long siblingUid; // 下一个兄弟节点的UID
    }

    /**
     * 叶子节点范围搜索
     * @param leftKey 左边界键值
     * @param rightKey 右边界键值
     * @return 叶子节点范围搜索结果对象
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock(); // 加读锁保证数据的一致性
        try {
            int noKeys = getRawNoKeys(raw); // 获取节点的键数量
            int kth = 0;
            while(kth < noKeys) { // 遍历节点的键
                long ik = getRawKthKey(raw, kth); // 获取第kth个键的值
                if(ik >= leftKey) { // 如果第kth个键大于等于左边界键值
                    break; // 跳出循环
                }
                kth ++; // 继续下一个键
            }
            List<Long> uids = new ArrayList<>(); // 创建存储UID的列表
            while(kth < noKeys) { // 遍历节点的键
                long ik = getRawKthKey(raw, kth); // 获取第kth个键的值
                if(ik <= rightKey) { // 如果第kth个键小于等于右边界键值
                    uids.add(getRawKthSon(raw, kth)); // 将第kth个子节点的UID添加到列表中
                    kth ++; // 继续下一个键
                } else {
                    break; // 跳出循环
                }
            }
            long siblingUid = 0;
            if(kth == noKeys) { // 如果遍历到了节点的最后一个键
                siblingUid = getRawSibling(raw); // 获取节点的兄弟节点UID
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes(); // 创建叶子节点范围搜索结果对象
            res.uids = uids; // 设置搜索结果的UID列表
            res.siblingUid = siblingUid; // 设置搜索结果的兄弟节点UID
            return res; // 返回搜索结果
        } finally {
            dataItem.rUnLock(); // 释放读锁
        }
    }

    /**
     * 插入并分裂的结果类
     */
    class InsertAndSplitRes {
        long siblingUid; // 兄弟节点的UID
        long newSon; // 新的子节点的UID
        long newKey; // 新的键值
    }

    /**
     * 插入并拆分方法
     * @param uid 节点ID
     * @param key 键
     * @return 插入并拆分结果
     * @throws Exception 异常
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false; // 初始化插入是否成功的标志
        Exception err = null; // 初始化异常对象
        InsertAndSplitRes res = new InsertAndSplitRes(); // 创建InsertAndSplitRes对象

        dataItem.before(); // 执行数据项的before方法，准备插入操作
        try {
            success = insert(uid, key); // 调用insert方法进行插入操作
            if(!success) {
                res.siblingUid = getRawSibling(raw); // 如果插入不成功，则获取兄弟节点的ID
                return res; // 返回结果对象
            }
            if(needSplit()) { // 如果需要进行拆分操作
                try {
                    SplitRes r = split(); // 调用split方法进行拆分操作
                    res.newSon = r.newSon; // 获取拆分后的新子节点
                    res.newKey = r.newKey; // 获取拆分后的新键
                    return res; // 返回结果对象
                } catch(Exception e) {
                    err = e; // 捕获拆分过程中的异常
                    throw e; // 抛出异常
                }
            } else {
                return res; // 返回结果对象
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID); // 如果没有异常且插入成功，则执行数据项的after方法
            } else {
                dataItem.unBefore(); // 否则执行数据项的unBefore方法
            }
        }
    }

    /**
     * 插入方法
     * @param uid 节点ID
     * @param key 键
     * @return 是否成功插入
     */
    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw); // 获取节点的键的数量
        int kth = 0; // 初始化键的索引
        while(kth < noKeys) { // 遍历键的索引
            long ik = getRawKthKey(raw, kth); // 获取第kth个键
            if(ik < key) {
                kth ++; // 如果第kth个键小于要插入的键，则继续遍历
            } else {
                break; // 否则跳出循环
            }
        }
        if(kth == noKeys && getRawSibling(raw) != 0) {
            return false; // 如果遍历到最后一个键且存在兄弟节点，则返回插入失败
        }

        if(getRawIfLeaf(raw)) { // 如果当前节点是叶子节点
            shiftRawKth(raw, kth); // 将第kth个键及其后面的键向后移动一个位置
            setRawKthKey(raw, key, kth); // 设置第kth个键为要插入的键
            setRawKthSon(raw, uid, kth); // 设置第kth个子节点为要插入的节点
            setRawNoKeys(raw, noKeys+1); // 更新节点的键的数量
        } else {
            long kk = getRawKthKey(raw, kth); // 获取第kth个键
            setRawKthKey(raw, key, kth); // 设置第kth个键为要插入的键
            shiftRawKth(raw, kth+1); // 将第kth+1个键及其后面的键向后移动一个位置
            setRawKthKey(raw, kk, kth+1); // 设置第kth+1个键为原第kth个键
            setRawKthSon(raw, uid, kth+1); // 设置第kth+1个子节点为要插入的节点
            setRawNoKeys(raw, noKeys+1); // 更新节点的键的数量
        }
        return true; // 返回插入成功
    }

    /**
     * 判断是否需要拆分
     * @return 是否需要拆分
     */
    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw); // 如果节点的键的数量等于平衡数的两倍，则需要进行拆分
    }

    class SplitRes {
        long newSon, newKey; // 定义一个包含新子节点和新键的SplitRes类
    }
    /**
     * 拆分方法
     * @return 拆分结果
     * @throws Exception 异常
     */
    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE); // 创建一个新的节点数组
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw)); // 设置新节点数组的叶子节点属性与原节点数组相同
        setRawNoKeys(nodeRaw, BALANCE_NUMBER); // 设置新节点数组的键的数量为平衡数
        setRawSibling(nodeRaw, getRawSibling(raw)); // 设置新节点数组的兄弟节点为原节点数组的兄弟节点
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER); // 将原节点数组的后半部分键和子节点复制到新节点数组
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw); // 将新节点数组插入到数据管理器中，并返回新节点的ID
        setRawNoKeys(raw, BALANCE_NUMBER); // 更新原节点数组的键的数量为平衡数
        setRawSibling(raw, son); // 设置原节点数组的兄弟节点为新节点的ID

        SplitRes res = new SplitRes(); // 创建SplitRes对象
        res.newSon = son; // 设置拆分后的新子节点为新节点的ID
        res.newKey = getRawKthKey(nodeRaw, 0); // 设置拆分后的新键为新节点数组的第一个键
        return res; // 返回拆分结果
    }

    /**
     * 将节点的信息转为字符串
     * @return 节点的字符串表示
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n"); // 添加是否是叶子节点的信息
        int KeyNumber = getRawNoKeys(raw); // 获取节点的键的数量
        sb.append("KeyNumber: ").append(KeyNumber).append("\n"); // 添加键的数量的信息
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n"); // 添加兄弟节点的信息
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n"); // 添加子节点和键的信息
        }
        return sb.toString(); // 返回节点的字符串表示
    }
}
