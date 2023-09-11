package com.rt.rtdb.backend.vm;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.rt.rtdb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 * @author RT666
 */
public class LockTable {

    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;                  // 用于并发访问控制的互斥锁

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 不需要等待则返回null，否则返回锁对象
     * 如果会造成死锁，则抛出异常
     *
     * @param xid 事务ID
     * @param uid 用户ID
     * @return 如果不需要等待，则返回null；否则返回锁对象
     * @throws Exception 如果会造成死锁，则抛出异常
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock(); // 获取锁
        try {
            if (isInList(x2u, xid, uid)) {
                return null; // 如果事务ID和用户ID已存在于列表中，则不需要等待，返回null
            }
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid); // 如果用户ID不存在于映射表中，则将用户ID和事务ID存入映射表
                putIntoList(x2u, xid, uid); // 将用户ID插入到事务ID的列表的开头
                return null; // 不需要等待，返回null
            }
            waitU.put(xid, uid); // 将事务ID和用户ID存入等待映射表
            putIntoList(wait, uid, xid); // 将事务ID插入到用户ID的列表的开头
            if (hasDeadLock()) {
                waitU.remove(xid); // 如果存在死锁，则从等待映射表中移除事务ID
                removeFromList(wait, uid, xid); // 从用户ID的列表中移除事务ID
                throw Error.DeadlockException; // 抛出死锁异常
            }
            Lock l = new ReentrantLock(); // 创建一个新的锁对象
            l.lock(); // 获取新的锁对象
            waitLock.put(xid, l); // 将事务ID和锁对象存入等待锁映射表
            return l; // 返回锁对象

        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 从列表中移除指定的元素
     *
     * @param xid 事务ID
     */
    public void remove(long xid) {
        lock.lock(); // 获取锁
        try {
            List<Long> l = x2u.get(xid); // 获取事务ID对应的用户ID列表
            if (l != null) {
                while (l.size() > 0) {
                    Long uid = l.remove(0); // 移除列表中的用户ID
                    selectNewXID(uid); // 选择一个新的事务ID占用该用户ID
                }
            }
            waitU.remove(xid); // 从等待映射表中移除事务ID
            x2u.remove(xid); // 从事务ID映射表中移除事务ID
            waitLock.remove(xid); // 从等待锁映射表中移除事务ID

        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 从等待队列中选择一个事务ID来占用用户ID
     *
     * @param uid 用户ID
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid); // 从用户ID映射表中移除用户ID
        List<Long> l = wait.get(uid); // 获取用户ID对应的等待事务ID列表
        if (l == null) {
            return;
        }
        assert l.size() > 0;

        while (l.size() > 0) {
            long xid = l.remove(0); // 移除列表中的事务ID
            if (!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid); // 将用户ID和事务ID存入映射表
                Lock lo = waitLock.remove(xid); // 从等待锁映射表中移除事务ID，并获取对应的锁对象
                waitU.remove(xid); // 从等待映射表中移除事务ID
                lo.unlock(); // 释放锁对象
                break;
            }
        }

        if (l.size() == 0) {
            wait.remove(uid); // 如果列表为空，则从等待映射表中移除用户ID
        }
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    /**
     * 检测是否存在死锁
     *
     * @return 如果存在死锁，则返回true；否则返回false
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>(); // 创建事务ID和时间戳的映射表
        stamp = 1; // 初始化时间戳
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid); // 获取事务ID对应的时间戳
            if (s != null && s > 0) {
                continue;
            }
            stamp++; // 增加时间戳
            if (dfs(xid)) {
                return true; // 如果存在环形依赖，则存在死锁，返回true
            }
        }
        return false; // 不存在死锁，返回false
    }

    /**
     * 深度优先搜索，用于检测是否存在环形依赖
     *
     * @param xid 起始事务ID
     * @return 如果存在环形依赖，则返回true；否则返回false
     */
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid); // 获取事务ID对应的时间戳
        if (stp != null && stp == stamp) {
            return true; // 如果时间戳存在且与当前时间戳相等，则存在环形依赖，返回true
        }
        if (stp != null && stp < stamp) {
            return false; // 如果时间戳存在但小于当前时间戳，则不存在环形依赖，返回false
        }
        xidStamp.put(xid, stamp); // 将事务ID与当前时间戳存入映射表

        Long uid = waitU.get(xid); // 获取事务ID对应的等待用户ID
        if (uid == null) {
            return false; // 如果等待用户ID为空，则不存在环形依赖，返回false
        }
        Long x = u2x.get(uid); // 获取等待用户ID对应的事务ID
        assert x != null;
        return dfs(x); // 递归调用dfs方法，继续检测下一个事务ID是否存在环形依赖
    }

    /**
     * 从列表中移除指定的元素
     *
     * @param listMap 列表的映射表
     * @param uid0    第一个元素的ID
     * @param uid1    第二个元素的ID
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0); // 获取第一个元素的列表
        if (l == null) {
            return; // 如果列表为空，则无需移除
        }
        Iterator<Long> i = l.iterator(); // 创建列表的迭代器
        while (i.hasNext()) {
            long e = i.next(); // 获取下一个元素
            if (e == uid1) {
                i.remove(); // 如果元素等于第二个元素的ID，则移除该元素
                break;
            }
        }
        if (l.size() == 0) {
            listMap.remove(uid0); // 如果列表为空，则从映射表中移除该列表
        }
    }

    /**
     * 将元素插入到列表的开头
     *
     * @param listMap 列表的映射表
     * @param uid0    第一个元素的ID
     * @param uid1    第二个元素的ID
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if (!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>()); // 如果映射表中不包含第一个元素的ID，则创建一个新的列表
        }
        listMap.get(uid0).add(0, uid1); // 将第二个元素的ID插入到第一个元素的列表的开头
    }
    /**
     * 判断元素是否存在于列表中
     *
     * @param listMap 列表的映射表
     * @param uid0    第一个元素的ID
     * @param uid1    第二个元素的ID
     * @return 如果第二个元素存在于第一个元素的列表中，则返回true；否则返回false
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0); // 获取第一个元素的列表
        if (l == null) {
            return false; // 如果列表为空，则第二个元素肯定不存在于列表中，返回false
        }

        Iterator<Long> i = l.iterator(); // 创建列表的迭代器
        while (i.hasNext()) {
            long e = i.next(); // 获取下一个元素
            if (e == uid1) {
                return true; // 如果第二个元素等于当前元素，则存在于列表中，返回true
            }
        }
        return false; // 遍历完整个列表都没有找到第二个元素，返回false
    }
}
