package com.example.model;

public class SyntheticCriteria {

    String schema;
    String table;
    String column;
    String condition;
    String value;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString(){

        return ("schema:" + this.schema + " table: " + this.table + " column: " + this.column+ 
        " condition: " + this.condition + " value: " + this.value); 
    }
}