package com.rt.rtdb.backend.common;

/**
 * 共享内存数组
 * @author ryh
 * @version 1.0
 * @since 1.0
 * @create 2023/8/10 15:50
 **/
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
