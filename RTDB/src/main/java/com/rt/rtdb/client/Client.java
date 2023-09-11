package com.rt.rtdb.client;

import com.rt.rtdb.transport.Packager;
import com.rt.rtdb.transport.Package;

/**
 * Client 类用于与数据库进行交互的客户端。
 * 它使用 RoundTripper 对象来发送数据包并接收响应。
 * @author RT666
 */
public class Client {
    private RoundTripper rt;

    /**
     * 构造函数，用于创建 Client 实例。
     * @param packager 用于封装数据传输的 Packager 对象
     */
    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    /**
     * 执行查询或操作，并返回结果。
     * @param stat 要执行的查询或操作的字节数组
     * @return 执行结果的字节数组
     * @throws Exception 执行过程中可能发生的异常
     */
    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if (resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    /**
     * 关闭 Client 对象，释放资源。
     */
    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
            // 处理关闭过程中的异常，可以根据实际情况进行适当的处理
        }
    }
}