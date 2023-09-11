package com.rt.rtdb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rt.rtdb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 * @author RT666
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程
    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            if(getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);//休眠1秒再继续尝试获取
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                
                continue;
            }

            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);// 引用计数+1
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count ++;// 增加缓存中元素的个数
            getting.put(key, true);// 标记正在获取该资源
            lock.unlock();
            break;
        }
        //从数据源获取资源
        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            count --;
            getting.remove(key);// 移除正在获取该资源的标记
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);// 移除正在获取该资源的标记
        cache.put(key, obj);// 将资源放入缓存
        references.put(key, 1);// 设置引用计数为1
        lock.unlock();
        
        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key)-1;// 减少了对该资源的一个引用
            if(ref == 0) {
                T obj = cache.get(key);//通过键 key 从缓存 cache 中获取资源对象 obj
                releaseForCache(obj); // 执行资源释放操作
                references.remove(key); // 移除引用计数
                cache.remove(key); // 移除缓存中的资源
                count --;// 减少缓存中元素的个数
            } else {
                references.put(key, ref);// 更新引用计数
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 安全关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();// 获取缓存中所有的键
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);// 移除引用计数
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
