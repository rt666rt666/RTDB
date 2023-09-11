package com.rt.rtdb.backend.im;

import com.rt.rtdb.backend.common.SubArray;
import com.rt.rtdb.backend.dm.DataManager;
import com.rt.rtdb.backend.dm.dataItem.DataItem;
import com.rt.rtdb.backend.tm.TransactionManagerImpl;
import com.rt.rtdb.backend.utils.Parser;
import com.rt.rtdb.backend.im.Node.InsertAndSplitRes;
import com.rt.rtdb.backend.im.Node.LeafSearchRangeRes;
import com.rt.rtdb.backend.im.Node.SearchNextRes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * B +树实现聚簇索引
 * @author ryh
 * @version 1.0
 * @since 1.0
 * @create 2023/7/30 10:59
 **/
public class BPlusTree {
    DataManager dm; // 数据管理器对象
    long bootUid; // 根节点的UID
    DataItem bootDataItem; // 根节点的数据项
    Lock bootLock; // 根节点的锁对象

    /**
     * 创建一个B+树
     * @param dm 数据管理器对象
     * @return 根节点的UID
     * @throws Exception 异常
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw(); // 创建一个新的空根节点的原始数据数组
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot); // 插入根节点的原始数据数组，并返回UID
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid)); // 将根节点的UID插入到数据管理器中，并返回UID
    }

    /**
     * 加载 B+树
     * @param bootUid 根节点的UID
     * @param dm 数据管理器
     * @return 加载的B+树
     * @throws Exception 加载B+树时可能抛出异常
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid); // 从数据管理器中读取根节点的数据项
        assert bootDataItem != null; // 确保读取的数据项不为空
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid; // 设置根节点的UID
        t.dm = dm; // 设置数据管理器
        t.bootDataItem = bootDataItem; // 设置启动数据项
        t.bootLock = new ReentrantLock(); // 创建启动锁
        return t; // 返回加载的B+树
    }

    /**
     * 获取根节点的UID
     * @return 根节点的UID
     */
    private long getRootUid() {
        bootLock.lock(); // 获取根节点的锁
        try {
            SubArray sa = bootDataItem.data(); // 获取根节点的数据数组
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8)); // 将根节点的UID从数据数组中解析并返回
        } finally {
            bootLock.unlock(); // 释放根节点的锁
        }
    }

    /**
     * 更新根节点的UID
     * @param left 左子节点的UID
     * @param right 右子节点的UID
     * @param rightKey 右子节点的键值
     * @throws Exception 异常
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock(); // 获取根节点的锁
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey); // 创建一个新的根节点的原始数据数组
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw); // 插入新的根节点的原始数据数组，并返回UID
            bootDataItem.before(); // 在更新根节点之前，标记根节点的数据项为脏数据
            SubArray diRaw = bootDataItem.data(); // 获取根节点的数据数组
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8); // 将新的根节点的UID转换为字节数组，并复制到根节点的数据数组中
            bootDataItem.after(TransactionManagerImpl.SUPER_XID); // 在更新根节点之后，标记根节点的数据项为干净数据
        } finally {
            bootLock.unlock(); // 释放根节点的锁
        }
    }

    /**
     * 在B+树中搜索叶子节点
     * @param nodeUid 节点的UID
     * @param key 键值
     * @return 叶子节点的UID
     * @throws Exception 异常
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid); // 加载节点
        boolean isLeaf = node.isLeaf(); // 判断节点是否为叶子节点
        node.release(); // 释放节点

        if(isLeaf) {
            return nodeUid; // 如果是叶子节点，则返回节点的UID
        } else {
            long next = searchNext(nodeUid, key); // 否则，搜索下一个节点
            return searchLeaf(next, key); // 递归搜索下一个节点
        }
    }

    /**
     * 在B+树中搜索下一个节点
     * @param nodeUid 节点的UID
     * @param key 键值
     * @return 下一个节点的UID
     * @throws Exception 异常
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid); // 加载节点
            SearchNextRes res = node.searchNext(key); // 搜索下一个节点
            node.release(); // 释放节点
            if(res.uid != 0) {
                return res.uid; // 如果找到下一个节点，则返回其UID
            }
            nodeUid = res.siblingUid; // 否则，继续搜索下一个节点
        }
    }

    /**
     * 在B+树中搜索指定键值的数据项
     * @param key 键值
     * @return 包含指定键值的数据项的UID列表
     * @throws Exception 异常
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key); // 调用searchRange方法，搜索指定键值范围内的数据项
    }

    /**
     * 在B+树中搜索指定键值范围的数据项
     * @param leftKey 左边界键值
     * @param rightKey 右边界键值
     * @return 包含指定键值范围内的数据项的UID列表
     * @throws Exception 异常
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = getRootUid(); // 获取根节点的UID
        long leafUid = searchLeaf(rootUid, leftKey); // 在B+树中搜索左边界键值所在的叶子节点
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid); // 加载叶子节点
            // 在叶子节点中搜索指定键值范围内的数据项
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release(); // 释放叶子节点
            uids.addAll(res.uids); // 将搜索到的数据项的UID添加到列表中
            if(res.siblingUid == 0) {
                break; // 如果没有下一个叶子节点，则跳出循环
            } else {
                leafUid = res.siblingUid; // 否则，继续搜索下一个叶子节点
            }
        }
        return uids; // 返回包含指定键值范围内的数据项的UID列表
    }

    /**
     * 在B+树中插入数据项
     * @param key 键值
     * @param uid 数据项的UID
     * @throws Exception 异常
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = getRootUid(); // 获取根节点的UID
        InsertRes res = insert(rootUid, uid, key); // 在B+树中插入数据项
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey); // 如果有新的根节点，则更新根节点的UID
        }
    }

    /**
     * 插入结果类
     */
    class InsertRes {
        long newNode, newKey;
    }

    /**
     * 在B+树中插入数据项
     * @param nodeUid 节点的UID
     * @param uid 数据项的UID
     * @param key 键值
     * @return 插入结果
     * @throws Exception 异常
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid); // 加载节点
        boolean isLeaf = node.isLeaf(); // 判断节点是否为叶子节点
        node.release(); // 释放节点

        InsertRes res = null;
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key); // 如果是叶子节点，则插入数据项并进行分裂
        } else {
            long next = searchNext(nodeUid, key); // 否则，搜索下一个节点
            InsertRes ir = insert(next, uid, key); // 在下一个节点中插入数据项
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey); // 如果有新的节点产生，则插入数据项并进行分裂
            } else {
                res = new InsertRes();
            }
        }
        return res; // 返回插入结果
    }

    /**
     * 插入数据项并进行分裂
     * @param nodeUid 节点的UID
     * @param uid 数据项的UID
     * @param key 键值
     * @return 插入结果
     * @throws Exception 异常
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid); // 加载节点
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key); // 插入数据项并进行分裂
            node.release(); // 释放节点
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid; // 如果有兄弟节点，则继续插入并分裂
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res; // 否则，返回插入结果
            }
        }
    }

    /**
     * 关闭B+树
     */
    public void close() {
        bootDataItem.release(); // 释放根节点的数据项
    }
}
