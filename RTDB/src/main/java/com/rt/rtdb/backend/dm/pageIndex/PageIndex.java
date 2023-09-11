package com.rt.rtdb.backend.dm.pageIndex;

import com.rt.rtdb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    // 将一页划分为40个区间
    private static final int INTERVALS_NO = 40;
    // 每个区间的大小阈值
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;
    private Lock lock;
    // 用于存储每个区间的页面信息列表的数组
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 将页面加入到索引中
     * @param pgno 页面编号
     * @param freeSpace 页面可用空间大小
     */
    public void add(int pgno, int freeSpace) {
        lock.lock(); // 获取锁，保证线程安全
        try {
            int number = freeSpace / THRESHOLD; // 计算页面所属的区间
            lists[number].add(new PageInfo(pgno, freeSpace)); // 将页面信息添加到对应的区间列表中
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 根据空间大小选择一个页面
     * @param spaceSize 需要的空间大小
     * @return 选中的页面信息
     */
    public PageInfo select(int spaceSize) {
        lock.lock(); // 获取锁，保证线程安全
        try {
            int number = spaceSize / THRESHOLD; // 计算所需空间大小
            if (number < INTERVALS_NO) { // 如果区间小于最大区间，向后移动一个区间
                number++;
            }
            while (number <= INTERVALS_NO) { // 在区间范围内查找非空列表
                if (lists[number].size() == 0) { // 如果列表为空，尝试下一个区间
                    number++;
                    continue;
                }
                return lists[number].remove(0); // 返回该区间的第一个页面信息，并从列表中移除
            }
            return null; // 所有区间都为空，返回null
        } finally {
            lock.unlock(); // 释放锁
        }
    }
}
