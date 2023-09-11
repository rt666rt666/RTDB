package com.rt.rtdb.backend.dm.page;

import com.rt.rtdb.backend.dm.pageCache.PageCache;
import com.rt.rtdb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {
    private static final int OF_VC = 100;  // 上一次数据库正常关闭标记的起始位置
    private static final int LEN_VC = 8;   // 上一次数据库正常关闭标记的长度

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);  // 初始化页面并设置为打开状态
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);  // 设置页面为脏页
        setVcOpen(pg.getData());  // 设置上一次数据库正常关闭标记为打开状态
    }

    private static void setVcOpen(byte[] raw) {
        // 给上一次数据库正常关闭标记填入随机字节
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);  // 设置页面为脏页
        setVcClose(pg.getData());  // 设置上一次数据库正常关闭标记为关闭状态
    }

    private static void setVcClose(byte[] raw) {
        // 将上一次数据库正常关闭标记拷贝到关闭状态位置
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());  // 检查上一次数据库正常关闭标记是否一致
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC),
                Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}
