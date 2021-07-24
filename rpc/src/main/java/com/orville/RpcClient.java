package com.orville;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Scanner;

import com.orville.protocol.MyInterface;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcClient {
    public static void main(String[] args){
        try{
            MyInterface proxy = RPC.getProxy(MyInterface.class, 1L, new InetSocketAddress("127.0.0.1", 12345), new Configuration());
            Scanner scan = new Scanner(System.in);
            System.out.println("please input your student number:");
            while(scan.hasNextLine()){
                String st = scan.nextLine();
                if (st=="exit" || st == "quit"){
                    break;
                }
                String name = proxy.findName(st);
                if (name == null){
                    System.out.println("null");
                }else{
                    System.out.println(name);
                }
            }
            scan.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}
