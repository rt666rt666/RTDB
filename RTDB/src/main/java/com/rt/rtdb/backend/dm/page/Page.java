package com.rt.rtdb.backend.dm.page;

public interface Page {
    // 锁定页面
    void lock();

    // 解锁页面
    void unlock();

    // 释放页面
    void release();

    // 设置页面的脏标记,在缓存驱逐的时候，脏页面需要被写回磁盘
    void setDirty(boolean dirty);

    // 检查页面是否被修改（脏）
    boolean isDirty();

    // 获取页面的页号
    int getPageNumber();

    // 获取页面的数据
    byte[] getData();
}

