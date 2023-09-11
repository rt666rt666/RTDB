package com.rt.rtdb.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 解析器工具类，提供字节转换和解析方法
 * @author ryh
 * @version 1.0
 * @since 1.0
 * @create 2023/7/5 11:59
 **/
public class Parser {

    /**
     * 将short值转换为字节数组
     * @param value 要转换的short值
     * @return 转换后的字节数组
     */
    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    /**
     * 解析字节数组为short值
     * @param buf 要解析的字节数组
     * @return 解析后的short值
     */
    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    /**
     * 将int值转换为字节数组
     * @param value 要转换的int值
     * @return 转换后的字节数组
     */
    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    /**
     * 解析字节数组为int值
     * @param buf 要解析的字节数组
     * @return 解析后的int值
     */
    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    /**
     * 解析字节数组为long值
     * @param buf 要解析的字节数组
     * @return 解析后的long值
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    /**
     * 将long值转换为字节数组
     * @param value 要转换的long值
     * @return 转换后的字节数组
     */
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    /**
     * 解析字节数组为字符串
     * @param raw 要解析的字节数组
     * @return 解析结果对象，包含解析后的字符串和字节数组长度
     */
    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }

    /**
     * 将字符串转换为字节数组
     * @param str 要转换的字符串
     * @return 转换后的字节数组
     */
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    /**
     * 将字符串转换为唯一标识符（long值）
     * @param key 要转换的字符串
     * @return 转换后的唯一标识符（long值）
     */
    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }

}
