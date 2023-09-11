package com.rt.rtdb.backend.tbm;

import com.rt.rtdb.backend.utils.Panic;
import com.rt.rtdb.common.Error;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Booter类用于记录第一个表的uid，并提供加载和更新功能。
 */
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    /**
     * 创建一个新的Booter实例并返回。
     * @param path 文件路径
     * @return 新的Booter实例
     */
    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);   // 如果文件已存在，则抛出异常
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);    // 如果文件不可读或不可写，则抛出异常
        }
        return new Booter(path, f);
    }

    /**
     * 打开指定路径下的Booter文件，并返回一个新的Booter实例。
     * @param path 文件路径
     * @return 新的Booter实例
     */
    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);    // 如果文件不存在，则抛出异常
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);    // 如果文件不可读或不可写，则抛出异常
        }
        return new Booter(path, f);
    }

    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    /**
     * 加载Booter文件中的数据，并以字节数组的形式返回。
     * @return 字节数组形式的数据
     */
    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());    // 读取文件内容到字节数组
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /**
     * 更新 Booter文件的内容为给定的数据。
     * @param data 需要更新的数据
     */
    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);    // 如果临时文件不可读或不可写，则抛出异常
        }
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);    // 写入数据到临时文件
            out.flush();
        } catch(IOException e) {
            Panic.panic(e);
        }
        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);    // 将临时文件移动到目标文件位置，替换目标文件
        } catch(IOException e) {
            Panic.panic(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);    // 如果目标文件不可读或不可写，则抛出异常
        }
    }

}
