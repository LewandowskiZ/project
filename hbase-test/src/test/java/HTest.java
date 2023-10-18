import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HTest {

    @Test
    public void test() throws IOException {

        Configuration entries = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(entries);

        Admin admin = connection.getAdmin();
        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();
        columnFamilyDescriptorList.add(ColumnFamilyDescriptorBuilder.newBuilder("info1".getBytes()).build());
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("bigdata:stu1")).setColumnFamilies(columnFamilyDescriptorList).build();

        admin.createTable(tableDescriptor);

        admin.close();

        Table table = connection.getTable(TableName.valueOf("bigdata:stu1"));
        Put put = new Put("1001".getBytes());
        put.addColumn("info1".getBytes(),"name".getBytes(),"zhangsan".getBytes());
        table.put(put);

        Delete delete = new Delete("1001".getBytes());

        delete.addFamily("info1".getBytes());
        delete.addColumns("info1".getBytes(),"name".getBytes());
        table.delete(delete);

        Get get = new Get("1001".getBytes());
        get.addFamily("info1".getBytes());
        get.addColumn("info1".getBytes(),"name".getBytes());
        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            new String(CellUtil.cloneRow(cell));
            new String(CellUtil.cloneFamily(cell));
            new String(CellUtil.cloneQualifier(cell));
        }

        Scan scan = new Scan();

        scan.withStartRow("1001".getBytes(),true);
        scan.withStopRow("1001".getBytes(),true);

        BinaryComparator binaryComparator = new BinaryComparator("zhangsan".getBytes());
        //ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, binaryComparator);

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("info1".getBytes(), "name".getBytes(), CompareOperator.EQUAL, binaryComparator);

        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);

        for (Result result1 : scanner) {
            for (Cell cell : result1.rawCells()) {
                new String(CellUtil.cloneRow(cell));
                new String(CellUtil.cloneFamily(cell));
                new String(CellUtil.cloneQualifier(cell));
            }
        }

        table.close();
    }
}
