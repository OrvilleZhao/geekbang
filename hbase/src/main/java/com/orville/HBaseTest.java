package com.orville;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;

/**
 * Hello world!
 */
public class HBaseTest {
    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // 加载hbase配置
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "jikehadoop01,jikehadoop02,jikehadoop03");
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        TableName tableName = TableName.valueOf("orville");
        try(Connection con = ConnectionFactory.createConnection(configuration);
        Admin admin = con.getAdmin()){
            String nameSpace = "orville";
            try{
                admin.getNamespaceDescriptor(nameSpace);
            }catch(NamespaceNotFoundException e){
                //找不到命名空间，则创建命名空间
                NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
                admin.createNamespace(namespaceDescriptor);
            }
            // 插入数据
            // name列簇
            ColumnFamilyDescriptor nameFamily = ColumnFamilyDescriptorBuilder.of("name");
            // info列簇
            ColumnFamilyDescriptor infoFamily = ColumnFamilyDescriptorBuilder.of("info");
            // score列簇
            ColumnFamilyDescriptor scorFamily = ColumnFamilyDescriptorBuilder.of("score");
            ArrayList<ColumnFamilyDescriptor> families = new ArrayList<ColumnFamilyDescriptor>();
            families.add(nameFamily);
            families.add(infoFamily);
            families.add(scorFamily);
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamilies(families).build();
            //创建表
            admin.createTable(desc);
            
            List<Put> puts = new ArrayList<>(5);
            // 第0行
            Put put = new Put(Bytes.toBytes(0));
            put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes("Orville"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("G20210735010488"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("5"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("80"));
            puts.add(put);
            try (Table table = con.getTable(tableName)) {
                table.put(puts);
            }
            // 查询数据
            Get get = new Get(Bytes.toBytes(0));
            get.addColumn(Bytes.toBytes("name"), Bytes.toBytes("name"));
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"));
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"));
            get.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"));
            get.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"));
            try (Table table = con.getTable(tableName)) {
                Result dbresult = table.get(get);
                for (Cell cell : dbresult.listCells()) {
                    System.out.println("column:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("-------------------------------");
                }
            }
        }
    }
}
