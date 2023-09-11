package com.rt.rtdb.backend.parser;

import com.rt.rtdb.common.Error;

/**
 * Tokenizer类用于将字节数组解析为字符串标记
 */
public class Tokenizer {
    private byte[] stat; // 字节数组
    private int pos; // 当前位置
    private String currentToken; // 当前标记
    private boolean flushToken; // 是否刷新标记
    private Exception err; // 异常信息

    /**
     * 构造函数，初始化Tokenizer对象
     *
     * @param stat 字节数组
     */
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 返回当前标记，但不移动位置
     *
     * @return 当前标记
     * @throws Exception 如果发生异常
     */
    public String peek() throws Exception {
        if(err != null) { // 如果存在异常，则抛出异常信息
            throw err;
        }
        if(flushToken) { // 如果需要刷新标记，则重新解析下一个标记并保存到currentToken中
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken; // 返回当前标记值
    }

    /**
     * 弹出当前标记，并移动到下一个标记
     */
    public void pop() {
        flushToken = true; // 设置flushToken为true，表示需要重新解析下一个标记
    }

    /**
     * 返回错误的字节数组
     *
     * @return 错误的字节数组
     */
    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    /**
     * 弹出一个字节，移动到下一个位置
     */
    private void popByte() {
        pos ++; // 移动到下一个位置
        if(pos > stat.length) { // 如果已经超过数组长度，则将pos设置为数组长度
            pos = stat.length;
        }
    }

    /**
     * 返回下一个字节，但不移动位置
     *
     * @return 下一个字节
     */
    private Byte peekByte() {
        if(pos == stat.length) { // 如果已经到达数组末尾，则返回null
            return null;
        }
        return stat[pos]; // 返回下一个字节值
    }

    /**
     * 返回下一个标记
     *
     * @return 下一个标记
     * @throws Exception 如果发生异常
     */
    private String next() throws Exception {
        if(err != null) { // 如果存在异常，则抛出异常信息
            throw err;
        }
        return nextMetaState(); // 返回下一个元状态标记值
    }

    /**
     * 返回下一个元状态
     *
     * @return 下一个元状态
     * @throws Exception 如果发生异常
     */
    private String nextMetaState() throws Exception {
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();
        if(isSymbol(b)) { // 如果是符号，则返回该符号作为标记
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') { // 如果是引号，则进入引号状态
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) { // 如果是字母或数字，则进入标记状态
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException; // 如果不满足以上条件，则抛出无效命令异常
            throw err;
        }
    }

    /**
     * 返回下一个标记状态
     *
     * @return 下一个标记状态
     * @throws Exception 如果发生异常
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder(); // 使用StringBuilder来构建标记字符串
        while(true) {
            Byte b = peekByte();
            // 如果当前字节为null或者不是字母、数字、下划线，则结束循环并返回已构建的标记字符串
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                // 如果当前字节为空白字符，则移动到下一个位置，继续解析下一个元状态
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString(); // 返回已构建的标记字符串
            }
            sb.append(new String(new byte[]{b}));
            popByte(); // 移动到下一个位置，继续解析下一个元状态
        }
    }

    /**
     * 判断字节是否为数字
     *
     * @param b 字节
     * @return 如果是数字返回true，否则返回false
     */
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    /**
     * 判断字节是否为字母
     *
     * @param b 字节
     * @return 如果是字母返回true，否则返回false
     */
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     * 返回下一个引号状态
     *
     * @return 下一个引号状态
     * @throws Exception 如果发生异常
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte(); // 获取当前引号字符
        popByte(); // 移动到下一个位置，准备解析引号内的字符串内容
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) { // 如果已经到达数组末尾，则抛出无效命令异常
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) { // 如果遇到与引号匹配的字符，则结束循环并返回已构建的字符串内容
                popByte(); // 移动到下一个位置，继续解析下一个元状态标记值或结束解析过程
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte(); // 移动到下一个位置，继续解析下一个字符内容
        }
        return sb.toString(); // 返回已构建的字符串内容作为标记值
    }

    /**
     * 判断字节是否为符号
     *
     * @param b 字节
     * @return 如果是符号返回true，否则返回false
     */
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    /**
     * 判断字节是否为空白字符
     *
     * @param b 字节
     * @return 如果是空白字符返回true，否则返回false
     */
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
