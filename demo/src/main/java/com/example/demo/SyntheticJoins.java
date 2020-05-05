package com.example.demo;

public class SyntheticJoins {

    String schema1;
    String table1;
    String column1;
    String static1;
    String maxCount1;
    String schema2;
    String table2;
    String column2;
    String static2;
    String maxCount2;

    public String getSchema1() {
        return schema1;
    }

    public void setSchema1(String schema1) {
        this.schema1 = schema1;
    }

    public String getTable1() {
        return table1;
    }

    public void setTable1(String table1) {
        this.table1 = table1;
    }

    public String getColumn1() {
        return column1;
    }

    public void setColumn1(String column1) {
        this.column1 = column1;
    }

    public String getStatic1() {
        return static1;
    }

    public void setStatic1(String static1) {
        this.static1 = static1;
    }

    public String getStatic2() {
        return static2;
    }

    public String getMaxCount1() {
        return maxCount1;
    }

    public void setMaxCount1(String maxCount1) {
        this.maxCount1 = maxCount1;
    }

    public String getSchema2() {
        return schema2;
    }

    public void setSchema2(String schema2) {
        this.schema2 = schema2;
    }

    public String getTable2() {
        return table2;
    }

    public void setTable2(String table2) {
        this.table2 = table2;
    }

    public String getColumn2() {
        return column2;
    }

    public void setColumn2(String column2) {
        this.column2 = column2;
    }

    public String getMaxCount2() {
        return maxCount2;
    }

    public void setMaxCount2(String maxCount2) {
        this.maxCount2 = maxCount2;
    }

    @Override
    public String toString(){

        return ("schema1:" + this.schema1 + " table1 : " + this.table1 + " column1: " + this.column1 + 
        " static1: " + this.static1 + " maxCount1: " + this.maxCount1 + " schema2: " + this.schema2 + 
        " table2: " + this.table2 + " column2: " + this.column2 + " static2: "
         + this.static2 + " maxCount2: " + this.maxCount2); 
    }
}