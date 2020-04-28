package com.example.demo;

public class JobDetails {

    int jobId;
    String status;
    String fromSchName;
    String fromPWD;
    String fromDB;
    String toSchName;
    String toPWD;
    String toDB;
    String tableName;
    String copyType;
    String textArea;
    String partition;
    String createTime;
    String endTime;
    String exportComments;
    String importComments;

    public String getExportComments() {
        return exportComments;
    }

    public void setExportComments(String exportComments) {
        this.exportComments = exportComments;
    }

    public String getImportComments() {
        return importComments;
    }

    public void setImportComments(String importComments) {
        this.importComments = importComments;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getFromSchName() {
        return fromSchName;
    }

    public void setFromSchName(String fromSchName) {
        this.fromSchName = fromSchName;
    }

    public String getFromPWD() {
        return fromPWD;
    }

    public void setFromPWD(String fromPWD) {
        this.fromPWD = fromPWD;
    }

    public String getFromDB() {
        return fromDB;
    }

    public void setFromDB(String fromDB) {
        this.fromDB = fromDB;
    }

    public String getToSchName() {
        return toSchName;
    }

    public void setToSchName(String toSchName) {
        this.toSchName = toSchName;
    }

    public String getToPWD() {
        return toPWD;
    }

    public void setToPWD(String toPWD) {
        this.toPWD = toPWD;
    }

    public String getToDB() {
        return toDB;
    }

    public void setToDB(String toDB) {
        this.toDB = toDB;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCopyType() {
        return copyType;
    }

    public void setCopyType(String copyType) {
        this.copyType = copyType;
    }

    public String getTextArea() {
        return textArea;
    }

    public void setTextArea(String textArea) {
        this.textArea = textArea;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }
}