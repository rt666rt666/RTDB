package com.rt.rtdb.transport;

/**
 * Packager 类用于封装数据传输的工具类。
 * 它包含了一个 Transporter 对象和一个 Encoder 对象，用于发送和接收数据包。
 * @author RT666
 */
public class Packager {
    private Transporter transporter;
    private Encoder encoder;

    /**
     * 构造函数，用于创建 Packager 实例。
     * @param transporter 用于传输数据的 Transporter 对象
     * @param encoder 用于编码和解码数据的 Encoder 对象
     */
    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    /**
     * 发送数据包
     * @param pkg 要发送的数据包
     * @throws Exception 发送过程中可能发生的异常
     */
    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transporter.send(data);
    }

    /**
     * 接收数据包
     * @return 接收到的数据包
     * @throws Exception 接收过程中可能发生的异常
     */
    public Package receive() throws Exception {
        byte[] data = transporter.receive();
        return encoder.decode(data);
    }

    /**
     * 关闭 Packager 对象，释放资源
     * @throws Exception 关闭过程中可能发生的异常
     */
    public void close() throws Exception {
        transporter.close();
    }
}