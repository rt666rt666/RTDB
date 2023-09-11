package com.rt.rtdb.client;

import com.rt.rtdb.transport.Packager;
import com.rt.rtdb.transport.Package;

/**
 * RoundTripper 类用于发送数据包并接收响应的往返行程。
 * @author RT666
 */
public class RoundTripper {
    private Packager packager;

    /**
     * 构造函数，用于创建 RoundTripper 实例。
     * @param packager 用于封装数据传输的 Packager 对象
     */
    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 发送数据包并接收响应。
     * @param pkg 要发送的数据包
     * @return 接收到的响应数据包
     * @throws Exception 发送和接收过程中可能发生的异常
     */
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    /**
     * 关闭 RoundTripper 对象，释放资源。
     * @throws Exception 关闭过程中可能发生的异常
     */
    public void close() throws Exception {
        packager.close();
    }
}