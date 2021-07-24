package com.orville.protocol;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

public class MyInterfaceImpl implements MyInterface {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        // TODO Auto-generated method stub
        return MyInterface.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String findName(String studentNumber) {
        // if the number is G20210735010488 return orville else return null
        System.out.println("receive studentnumber:" + studentNumber);
        if (studentNumber.equals("G20210735010488")){
            return "orville";
        }
        return null;
    }
    
}