package ir.jimbo.web.graph.config;

import java.io.IOException;

public class HBaseConfiguration extends Config {

    private String tableName;
    private String columnName;

    public HBaseConfiguration(String prefix) throws IOException {
        super(prefix);
        tableName = getPropertyValue("table.name");
        columnName = getPropertyValue("column.family");
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTableName() {
        return tableName;
    }
}
