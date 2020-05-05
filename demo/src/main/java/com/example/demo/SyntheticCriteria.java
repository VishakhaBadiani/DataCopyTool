package com.example.demo;

public class SyntheticCriteria {

    String schema;
    String tab;
    String col;
    String condition;
    String value;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTab() {
        return tab;
    }

    public void setTab(String tab) {
        this.tab = tab;
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
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

        return ("schema:" + this.schema + " table: " + this.tab + " column: " + this.col+
        " condition: " + this.condition + " value: " + this.value); 
    }
}