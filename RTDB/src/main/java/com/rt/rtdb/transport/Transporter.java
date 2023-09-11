package com.rt.rtdb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * 传输器，用于在Socket连接上进行数据的发送和接收
 * 数据的发送和接收使用十六进制编码和解码
 * 注意：在接收数据时，如果读取的行为null，表示连接已关闭，将自动关闭连接
 * 在使用完Transporter后，需要调用close()方法显式关闭连接
 *
 * @author RT666
 */
public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    /**
     * 构造函数，创建Transporter对象并初始化输入输出流
     *
     * @param socket 要使用的Socket连接
     * @throws IOException 如果初始化输入输出流时发生IO异常
     */
    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 发送字节数组数据到连接的另一端
     *
     * @param data 要发送的字节数组
     * @throws Exception 如果发送数据时发生异常
     */
    public void send(byte[] data) throws Exception {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    /**
     * 接收连接的另一端发送的字节数组数据
     *
     * @return 接收到的字节数组数据
     * @throws Exception 如果接收数据时发生异常
     */
    public byte[] receive() throws Exception {
        String line = reader.readLine();
        if (line == null) {
            close();
        }
        return hexDecode(line);
    }

    /**
     * 关闭连接及相关的输入输出流
     *
     * @throws IOException 如果关闭连接时发生IO异常
     */
    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    /**
     * 将字节数组转换为十六进制表示的字符串
     *
     * @param buf 要编码的字节数组
     * @return 十六进制表示的字符串
     */
    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true) + "\n";
    }

    /**
     * 将十六进制表示的字符串解码为字节数组
     *
     * @param buf 十六进制表示的字符串
     * @return 解码后的字节数组
     * @throws DecoderException 如果解码失败
     */
    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}