package com.rt.rtdb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * RandomUtil 类用于生成指定长度的随机字节数组。
 */
public class RandomUtil {

    /**
     * randomBytes 方法用于生成指定长度的随机字节数组。
     *
     * @param length 生成的字节数组的长度
     * @return 生成的随机字节数组
     */
    public static byte[] randomBytes(int length) {
        // 创建一个 SecureRandom 对象
        Random r = new SecureRandom();

        // 创建一个长度为 length 的 byte 数组
        byte[] buf = new byte[length];

        // 使用 SecureRandom 对象生成随机字节，并将其填充到 buf 数组中
        r.nextBytes(buf);

        // 返回生成的随机字节数组
        return buf;
    }
}