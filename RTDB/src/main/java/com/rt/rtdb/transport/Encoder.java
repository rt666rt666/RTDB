package com.rt.rtdb.transport;

import com.google.common.primitives.Bytes;

import java.util.Arrays;
import com.rt.rtdb.common.Error;

/**
 * 编码器，用于将Package对象编码成字节数组，以及将字节数组解码为Package对象。
 * 编码规则：如果Package对象中存在错误，则将错误信息编码为字节数组；否则，将数据部分编码为字节数组。
 * 解码规则：根据字节数组的第一个字节判断编码类型，0表示数据部分，1表示错误信息。
 * 注意：字节数组的第一个字节用于标识编码类型。
 *
 * @author RT666
 */
public class Encoder {

    /**
     * 将Package对象编码为字节数组
     *
     * @param pkg 要编码的Package对象
     * @return 编码后的字节数组
     */
    public byte[] encode(Package pkg) {
        if (pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if (err.getMessage() != null) {
                msg = err.getMessage();
            }
            // 使用平台的默认字符集将字符串编码为字节数组，并将结果与新的字节数组拼接
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    /**
     * 将字节数组解码为Package对象
     *
     * @param data 要解码的字节数组
     * @return 解码后的Package对象
     * @throws Exception 当解码失败或数据不合法时抛出异常
     */
    public Package decode(byte[] data) throws Exception {
        if (data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        if (data[0] == 0) {
            // 解码数据部分，将字节数组中第一个字节后的部分作为数据
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if (data[0] == 1) {
            // 解码错误信息，将字节数组中第一个字节后的部分作为错误信息的字节数组，并转换为字符串
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }
}