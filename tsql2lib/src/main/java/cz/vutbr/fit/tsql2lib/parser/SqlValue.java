package cz.vutbr.fit.tsql2lib.parser;

public class SqlValue extends SimpleNode
{
    String value;
    public SqlValue(String _value) { super(0); value = _value; }
    public String toString() { return ":"+value; }
    public String getValue() { return value; }
}
