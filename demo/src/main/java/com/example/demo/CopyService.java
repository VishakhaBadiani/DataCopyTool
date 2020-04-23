package com.example.demo;

import model.JobDetails;
import org.apache.poi.ss.usermodel.*;
import org.springframework.stereotype.Component;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class CopyService {

    String excelFilePath = "D:\\Vishakha\\Job_Details.xlsx";
    String dmpFilePath = "D:\\Vishakha\\Files\\";
    Workbook wb;
    Properties configProp = new Properties();
    InputStream in = this.getClass().getClassLoader().getResourceAsStream("application.properties");
    String value;

    public Integer writeToFile(String fromDB, String fromSchName, String toDB, String toSchName, String tableName, String copyType){
        int jobId=0;
        try{
            wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
            Sheet sheet = wb.getSheetAt(0);
            jobId = sheet.getLastRowNum()+1;
            System.out.println("Job Id - "+jobId);
            Row row = sheet.createRow(jobId);
            row.createCell(0).setCellValue(jobId);
            row.createCell(1).setCellValue(fromDB);
            row.createCell(2).setCellValue(fromSchName);
            row.createCell(3).setCellValue(toDB);
            row.createCell(4).setCellValue(toSchName);
            row.createCell(5).setCellValue(tableName);
            if(copyType.equalsIgnoreCase("TC")){
                copyType="Table Copy";
            }else if(copyType.equalsIgnoreCase("PC")){
                copyType="Partition Copy";
            }else if(copyType.equalsIgnoreCase("CC")){
                copyType="Customized Copy";
            }
            row.createCell(6).setCellValue(copyType);
            System.out.println((new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())));
            Cell cell = row.createCell(7);
            cell.setCellValue(/*Calendar.getInstance()*/new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            Cell cell2 = row.createCell(8);
            cell2.setCellValue(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            row.createCell(9).setCellValue("In Progress");
            row.createCell(10).setCellValue("Export in Progress");
            row.createCell(11).setCellValue("Import in Progress");
            FileOutputStream fileOut = new FileOutputStream(excelFilePath);
            wb.write(fileOut);
            fileOut.close();
        }catch(IOException e){
            throw new RuntimeException("Failed when writing to file. Please contact admin", e);
        }
        return jobId;
    }
    public void export(String user, String password, String fromDb, String tableName, int jobId, String copyType, String partition, String textArea) throws IOException {
        configProp.load(in);
        value = configProp.getProperty(fromDb);
        System.out.println("Copying Table - "+tableName +"for copyType - "+copyType);
        Process p = null;
        ProcessBuilder builder1 = null;
        if(copyType.equalsIgnoreCase("TC")){
            builder1 = new ProcessBuilder("D:\\app\\Vishakha\\product\\11.2.0\\dbhome_1\\BIN\\exp",
                    user+"/"+user+"@"+value, "tables="+tableName, "file="+jobId+".dmp", "direct=y", "log="+jobId+"_export.txt");
        }else if(copyType.equalsIgnoreCase("PC")){
            builder1 = new ProcessBuilder("D:\\app\\Vishakha\\product\\11.2.0\\dbhome_1\\BIN\\exp",
                    user+"/"+user+"@"+value, "tables="+tableName+":"+partition, "file="+jobId+".dmp", "direct=y", "log="+jobId+"_export.txt");
        }else if(copyType.equalsIgnoreCase("CC")){
            Formatter x= new Formatter("D:\\Vishakha\\Files\\copy.par");
            x.format("tables="+tableName);
            x.format(" file="+jobId+".dmp");
            x.format(" log="+jobId+"_export.txt");
            x.format(" query="+textArea);
            x.close();
            builder1 = new ProcessBuilder("D:\\app\\Vishakha\\product\\11.2.0\\dbhome_1\\BIN\\exp",
                    user+"/"+user+"@"+value, "parfile=copy.par");
        }
        builder1.directory(new File("D:\\Vishakha\\Files\\"));
        builder1.environment().put("ORACLE_HOME", "D:\\app\\Vishakha\\product\\11.2.0\\dbhome_1");
        builder1.environment().put("PATH", "%ORACLE_HOME%\\BIN;%PATH%");

        builder1.redirectErrorStream(true);
        try {
            wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
            Sheet firstSheet = wb.getSheetAt(0);
            Cell cell3 = firstSheet.getRow(jobId).getCell(9);
            Cell cell = firstSheet.getRow(jobId).getCell(10);
            p = builder1.start();
            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while (true) {
                line = r.readLine();
                if (line == null) { break; }
                System.out.println(line);
                if(line.contains("exported")){
                    cell.setCellValue(line);
                }else if(line.contains("unsuccessfully")){
                    cell.setCellValue(line);
                    cell3.setCellValue("Failed");
                }
            }
            Cell cell2 = firstSheet.getRow(jobId).getCell(8);
            cell2.setCellValue(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            FileOutputStream fileOut = new FileOutputStream(excelFilePath);
            wb.write(fileOut);
            fileOut.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed when exporting data. Please contact admin", e);
        }
    }

    public void importData(String toSch, String toPwd, String toDB, int jobId){
        try{
            configProp.load(in);
            value = configProp.getProperty(toDB);
            wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
            Sheet firstSheet = wb.getSheetAt(0);
            Cell cell3 = firstSheet.getRow(jobId).getCell(11);
            Cell cell = firstSheet.getRow(jobId).getCell(9);
            Process p = null;
            ProcessBuilder builder1 = new ProcessBuilder("D:\\app\\Vishakha\\product\\11.2.0\\dbhome_1\\BIN\\imp", toSch+"/"+toSch+"@"+value, "file="+jobId+".dmp",
                    "full=y", "log="+jobId+"_import.txt", "ignore=y");
            builder1.directory(new File("D:\\Vishakha\\Files\\"));
            builder1.environment().put("ORACLE_HOME", "D:\\app\\Vishakha\\product\\11.2.0\\dbhome_1");
            builder1.environment().put("PATH", "%ORACLE_HOME%\\BIN;%PATH%");
            builder1.redirectErrorStream(true);
            p = builder1.start();
            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while (true) {
                line = r.readLine();
                if (line == null) { break; }
                System.out.println(line);
                if(line.contains("imported")){
                    cell3.setCellValue(line);
                    cell.setCellValue("Completed");
                }
                else if(line.contains("failed")){
                    cell3.setCellValue(line);
                    cell.setCellValue("Failed");
                }
            }
            Cell cell2 = firstSheet.getRow(jobId).getCell(8);
            cell2.setCellValue(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            FileOutputStream fileOut = new FileOutputStream(excelFilePath);
            wb.write(fileOut);
            fileOut.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed when importing data. Please contact admin", e);
        }
    }
    public List<JobDetails> readFromFile(){
        List<JobDetails> jobDetailsList = new ArrayList<>();
        try{
            int rowCount=0;
            wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
            Sheet firstSheet = wb.getSheetAt(0);
            Iterator<Row> iterator = firstSheet.iterator();
            while(iterator.hasNext()){
                Row nextRow = iterator.next();
                Iterator<Cell> cellIterator = nextRow.cellIterator();
                JobDetails jobDetails=new JobDetails();
                if(rowCount==0){
                    System.out.println("Skipping this row");
                    ++rowCount;
                }
                else{
                    while(cellIterator.hasNext()){
                        Cell nextCell = cellIterator.next();
                        int columnIndex = nextCell.getColumnIndex();
                        switch (columnIndex){
                            case 0:
                                jobDetails.setJobId((int) nextCell.getNumericCellValue());
                                break;
                            case 1:
                                jobDetails.setFromDB(nextCell.getStringCellValue());
                                break;
                            case 2:
                                jobDetails.setFromSchName(nextCell.getStringCellValue());
                                break;
                            case 3:
                                jobDetails.setToDB(nextCell.getStringCellValue());
                                break;
                            case 4:
                                jobDetails.setToSchName(nextCell.getStringCellValue());
                                break;
                            case 5:
                                jobDetails.setTableName(nextCell.getStringCellValue());
                                break;
                            case 6:
                                jobDetails.setCopyType(nextCell.getStringCellValue());
                                break;
                            case 7:
                                jobDetails.setCreateTime(nextCell.getStringCellValue());
                                break;
                            case 8:
                                jobDetails.setEndTime(nextCell.getStringCellValue());
                                break;
                            case 9:
                                jobDetails.setStatus(nextCell.getStringCellValue());
                                break;
                            case 10:
                                jobDetails.setExportComments(nextCell.getStringCellValue());
                                break;
                            case 11:
                                jobDetails.setImportComments(nextCell.getStringCellValue());
                                break;
                        }
                    }
                }
                if(jobDetails.getJobId()!=0){
                    jobDetailsList.add(jobDetails);
                }}
        }catch(IOException e){
            throw new RuntimeException("Failed when reading the file.", e);
        }
        return jobDetailsList;
    }

    public List<String> getValues(String key){
        String valueSeperated[];
        List<String> valueList= new ArrayList<>();
        try {
            configProp.load(in);
            value = configProp.getProperty(key);
            valueSeperated=value.split(",");
            for(String s: valueSeperated){
                valueList.add(s);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed when fetching values from property file.", e);
        }
        return valueList;
    }

    public void deleteFile(int jobId){
        File myObj = new File(dmpFilePath+jobId+".dmp");
        if (myObj.delete()) {
            System.out.println("Deleted the dmp file: " + myObj.getName());
        } else {
            System.out.println("Failed to delete the file.");
        }
    }
}