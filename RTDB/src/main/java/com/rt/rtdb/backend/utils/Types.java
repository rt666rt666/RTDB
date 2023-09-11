package com.rt.rtdb.backend.utils;

/**
 * Types 类用于提供一些类型转换相关的方法。
 */
public class Types {

    /**
     * addressToUid 方法用于将页号（pgno）和偏移量（offset）转换为唯一标识符（uid）。
     *
     * @param pgno    页号
     * @param offset  偏移量
     * @return        转换后的唯一标识符
     */
    public static long addressToUid(int pgno, short offset) {
        // 将页号和偏移量转换为 long 类型
        long u0 = (long) pgno;
        long u1 = (long) offset;

        // 将 u0 左移 32 位，然后与 u1 进行按位或操作，得到唯一标识符
        return u0 << 32 | u1;
    }
}
