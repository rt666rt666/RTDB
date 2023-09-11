package com.rt.rtdb.backend.utils;

/**
 * 解析字符串结果类
 * @author RT666
 */
public class ParseStringRes {
    public String str; // 解析后的字符串
    public int next; // 下一个位置

    /**
     * 构造方法
     *
     * @param str 解析后的字符串
     * @param next 下一个位置
     */
    public ParseStringRes(String str, int next) {
        this.str = str; // 初始化解析后的字符串
        this.next = next; // 初始化下一个位置
    }
}

