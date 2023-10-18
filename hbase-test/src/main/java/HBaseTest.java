import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTest {


    public static void main(String[] args) throws IOException {

        Connection connection = getConnection();

        //createTable(connection, "bigdata:stu", "info1", "info2");

        //dropTable(connection, "bigdata:stu");

        //putData(connection, "bigdata:stu", "1002", "info2", "age", "12");

        //getData(connection, "bigdata:stu", "1001", "info2", "name");

        //scanTable(connection, "bigdata:stu");

        deleteData(connection, "bigdata:stu", "1001", "info2", "name");

        connection.close();
    }

    private static Connection getConnection() throws IOException {

        // 获取配置（ZK）
        Configuration configuration = HBaseConfiguration.create();
        // 获取连接
        return ConnectionFactory.createConnection(configuration);
    }

    private static void createTable(Connection connection, String nAndT, String... cfs) throws IOException {

        if (cfs.length <= 0) {
            System.out.println("请至少设置一个列族！");
            return;
        }

        // Admin对象用于操作DDL
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(nAndT);

        if (admin.tableExists(tableName)) {

            System.out.println("待创建的表" + tableName + "已存在！");
            return;
        }

        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();
        for (int i = 0; i < cfs.length; i++) {

            // 创建列族描述器
            columnFamilyDescriptorList.add(ColumnFamilyDescriptorBuilder.newBuilder(cfs[i].getBytes()).build());
        }

        // 创建表描述器
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(columnFamilyDescriptorList).build();

        // 创建表
        admin.createTable(tableDescriptor);

        admin.close();
    }

    private static void dropTable(Connection connection, String nAndT) throws IOException {

        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(nAndT);

        if (!admin.tableExists(tableName)) {

            System.out.println("待删除的表" + tableName + "不存在！");
            return;
        }

        admin.disableTable(tableName);
        admin.deleteTable(tableName);

        admin.close();
    }

    private static void putData(Connection connection, String nAndT, String rowKey, String cf, String cn, String value) throws IOException {

        // Table对象用于操作DML
        Table table = connection.getTable(TableName.valueOf(nAndT));

        // 创建Put对象
        Put put = new Put(rowKey.getBytes());
        // 设置列信息
        put.addColumn(cf.getBytes(), cn.getBytes(), value.getBytes());
        table.put(put);

        table.close();
    }

    private static void getData(Connection connection, String nAndT, String rowKey, String cf, String cn) throws IOException {

        // Table对象用于操作DML
        Table table = connection.getTable(TableName.valueOf(nAndT));

        // 创建Get对象
        Get get = new Get(rowKey.getBytes());
        // 设置列信息
        //get.addFamily(cf.getBytes());
        //get.addColumn(cf.getBytes(), cn.getBytes());

        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey:" + new String(CellUtil.cloneRow(cell)) +
                    ",CF:" + new String(CellUtil.cloneFamily(cell)) +
                    ",CN:" + new String(CellUtil.cloneQualifier(cell)));
        }

        table.close();
    }

    private static void scanTable(Connection connection, String nAndT) throws IOException {

        // Table对象用于操作DML
        Table table = connection.getTable(TableName.valueOf(nAndT));

        // 构建Scan对象
        Scan scan = new Scan();

        // 指定扫描范围
        // scan.withStartRow("1001".getBytes(), true);
        // scan.withStopRow("1002".getBytes(), true);

        // 指定过滤器
        ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new BinaryComparator("zhangsan".getBytes()));
        // 匹配上的和没有该列的整行数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("info1".getBytes(), "name".getBytes(), CompareOperator.EQUAL, new BinaryComparator("zhangsan".getBytes()));
        // (a and b) or c
//        FilterList filterListAB = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        filterListAB.addFilter(a);
//        filterListAB.addFilter(b);
//        FilterList filterListABC = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        filterListABC.addFilter(filterListAB);
//        filterListABC.addFilter(c);

        scan.setFilter(valueFilter);

        // 执行扫描表的操作
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey:" + new String(CellUtil.cloneRow(cell)) +
                        ",CF:" + new String(CellUtil.cloneFamily(cell)) +
                        ",CN:" + new String(CellUtil.cloneQualifier(cell)));
            }
        }

        table.close();
    }

    private static void deleteData(Connection connection, String nAndT, String rowKey, String cf, String cn) throws IOException {

        // Table对象用于操作DML
        Table table = connection.getTable(TableName.valueOf(nAndT));

        // 创建Delete对象
        Delete delete = new Delete(rowKey.getBytes());
        // 指定列族
        delete.addFamily(cf.getBytes());
        // 指定列
        //delete.addColumn(cf.getBytes(),cn.getBytes());
        delete.addColumns(cf.getBytes(), cn.getBytes());
        table.delete(delete);

        table.close();
    }
}
