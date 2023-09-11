package com.rt.rtdb.client;

import java.util.Scanner;

/**
 * 运行数据库交互的命令行界面
 * @author RT666
 */
public class Shell {
    private Client client;

    /**
     * 构造函数，用于创建 Shell 实例。
     * @param client 用于与数据库进行交互的 Client 对象
     */
    public Shell(Client client) {
        this.client = client;
    }

    /**
     * 运行命令行界面
     */
    public void run() {
        Scanner sc = new Scanner(System.in);
        try {
            while (true) {
                System.out.print(":> ");
                String statStr = sc.nextLine();
                if ("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    byte[] res = client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } finally {
            sc.close();
            client.close();
        }
    }
}