package cz.vutbr.fit.tsql2lib.parser;

public class SqlValue extends SimpleNode {

    String sqlStringValue;

    public SqlValue(String _value) {
        super(0);
        sqlStringValue = _value;
    }

    @Override
    public String toString() {
        return ":" + sqlStringValue;
    }

    public String getValue() {
        return sqlStringValue;
    }
}
