package com.example.model;

import org.apache.poi.ss.usermodel.*;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class CopyService {

    String filePath = System.getenv("DCT_HOME");
    String dmpFilePath=filePath+"\\Files\\";
    String excelFilePath = filePath + "\\Job_Details.xlsx";
    String oraPath = System.getenv("ORACLE_HOME");
    private static final Logger logger = LoggerFactory.getLogger(CopyService.class);
    Workbook wb;
    Properties configProp = new Properties();
    InputStream in = this.getClass().getClassLoader().getResourceAsStream("application.properties");
    String value;
    String message;
       
    public Integer writeToFile(String fromDB, String fromSchName, String toDB, String toSchName, String tableName, String copyType){
        int jobId=0;
        try{
            wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
            Sheet sheet = wb.getSheetAt(0);
            jobId = sheet.getLastRowNum()+1;
            System.out.println("Job Id - "+jobId);
            logger.info("Job Id - "+jobId);
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
            }else if(copyType.equalsIgnoreCase("SDC")){
                copyType="Synthetic Data Creation";
            }
            row.createCell(6).setCellValue(copyType);
            logger.info((new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())));
            
            Cell cell = row.createCell(7);
            cell.setCellValue(/*Calendar.getInstance()*/new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            Cell cell2 = row.createCell(8);
            cell2.setCellValue(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            row.createCell(9).setCellValue("In Progress");
            if(!copyType.contains("Synthetic")){
                row.createCell(10).setCellValue("Export in Progress");
                row.createCell(11).setCellValue("Import in Progress");
            }else{
                row.createCell(10).setCellValue("NA");
                row.createCell(11).setCellValue("NA");
            }
            FileOutputStream fileOut = new FileOutputStream(excelFilePath);
            wb.write(fileOut);
            fileOut.close();
        }catch(IOException e){
            logger.error(e.getMessage(),e.getStackTrace());
            throw new RuntimeException("Failed when writing to file. Please contact admin", e);
        }
        return jobId;
    }
    public void export(String user, String password, String fromDb, String tableName, int jobId, String copyType, String partition, String textArea, String fromSid, Connection conn) throws IOException, SQLException {
        System.out.println("Copying Table - "+tableName +"for copyType - "+copyType);
        logger.info("Copying Table - "+tableName +"for copyType - "+copyType);
        Process p = null;
        ProcessBuilder builder1 = null;
        if(copyType.equalsIgnoreCase("TC")){
            builder1 = new ProcessBuilder(oraPath + "\\BIN\\exp",
                    user+"/"+password+"@"+fromSid, "tables="+tableName, "file="+jobId+".dmp", "direct=y", "log="+jobId+"_export.txt");
        }else if(copyType.equalsIgnoreCase("PC")){
            /*builder1 = new ProcessBuilder(oraPath + "\\BIN\\exp",
                    user+"/"+password+"@"+fromSid, "tables="+tableName+":"+partition, "file="+jobId+".dmp", "direct=y", "log="+jobId+"_export.txt");*/
            builder1 = new ProcessBuilder(oraPath + "\\BIN\\expdp",
                    user+"/"+password+"@"+fromSid, "tables="+tableName+":"+partition, "dumpfile="+tableName+"_"+partition+".dmp", "logfile="+jobId+"_export.txt");
        }else if(copyType.equalsIgnoreCase("CC")){
            /*Formatter x= new Formatter(dmpFilePath + "\\copy.par");
            x.format("tables="+tableName);
            x.format(" file="+jobId+".dmp");
            x.format(" log="+jobId+"_export.txt");
            x.format(" query="+textArea);
            x.close();
            builder1 = new ProcessBuilder(oraPath + "\\BIN\\exp",
                    user+"/"+password+"@"+fromSid, "parfile=copy.par");*/

            Statement st = conn.createStatement();
            st.executeQuery("DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = 'st1'");
            st.executeUpdate("BEGIN EXECUTE IMMEDIATE 'EXPLAIN PLAN SET STATEMENT_ID = ''st1'' FOR "+textArea+"'; END;");
            String sql = "WITH OBJ_NAME AS (SELECT a.object_name,object_alias,rn,LISTAGG (b.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY b.COLUMN_NAME)    col,\n" +
                    "         LISTAGG (object_alias||'.'||b.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY b.COLUMN_NAME)    col_alias\n" +
                    "    FROM (SELECT DISTINCT P.OBJECT_NAME,REPLACE (REGEXP_SUBSTR (OBJECT_ALIAS,'([^@]+)', 1, 1, NULL,1),'\"',NULL)    OBJECT_ALIAS,\n" +
                    "                          ROWNUM RN FROM PLAN_TABLE P\n" +
                    "           WHERE     P.OBJECT_NAME IS NOT NULL AND P.OBJECT_NAME NOT LIKE 'SYS%' AND OBJECT_OWNER <> 'SYS') a, user_tab_columns b\n" +
                    "   WHERE a.object_name = b.table_name GROUP BY a.object_name, object_alias, rn) " +
                    "SELECT    'WHERE ('||col||') IN (SELECT DISTINCT ' || col_ALIAS || ' FROM ' || (SELECT LISTAGG (ojn.OBJECT_NAME || ' ' || ojn.OBJECT_ALIAS, ', ')" +
                    " WITHIN GROUP (ORDER BY ojn.OBJECT_NAME || ' ' || ojn.OBJECT_ALIAS) FROM OBJ_NAME ojn)\n" +
                    "       || CASE WHEN (SELECT DISTINCT LISTAGG (REPLACE (NVL (ACCESS_PREDICATES, FILTER_PREDICATES),'\"',NULL), ' AND ') WITHIN GROUP (ORDER BY NVL (ACCESS_PREDICATES, FILTER_PREDICATES)) FROM PLAN_TABLE\n" +
                    "                     WHERE STATEMENT_ID = 'st1' AND NVL (ACCESS_PREDICATES, FILTER_PREDICATES) IS NOT NULL) IS NOT NULL\n" +
                    "              THEN\n" +
                    "                     ' WHERE ' || REPLACE((SELECT DISTINCT LISTAGG (REPLACE ( NVL (ACCESS_PREDICATES, FILTER_PREDICATES), '\"', NULL), ' AND ') WITHIN GROUP (ORDER BY NVL (ACCESS_PREDICATES, FILTER_PREDICATES))\n" +
                    "                        FROM PLAN_TABLE WHERE STATEMENT_ID = 'st1' AND NVL (ACCESS_PREDICATES, FILTER_PREDICATES) IS NOT NULL),'INTERNAL_FUNCTION',NULL)\n" +
                    "          END || ')'    QUERY1\n" +
                    "  FROM OBJ_NAME  OJ,\n" +
                    "       (    SELECT LEVEL     LVL\n" +
                    "              FROM DUAL\n" +
                    "        CONNECT BY LEVEL <= (SELECT COUNT (*) FROM OBJ_NAME)) X\n" +
                    " WHERE OJ.RN = X.LVL";
            ResultSet rs = st.executeQuery(sql);
            while(rs.next()){
                System.out.println(rs.getString(1));
            }

        }
        builder1.directory(new File(dmpFilePath));
        builder1.environment().put("ORACLE_HOME", oraPath);
        builder1.environment().put("PATH", "%ORACLE_HOME%\\BIN;%PATH%");

        builder1.redirectErrorStream(true);
        try {
            wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
            Sheet firstSheet = wb.getSheetAt(0);
            Cell cell3 = firstSheet.getRow(jobId).getCell(9);
            Cell cell = firstSheet.getRow(jobId).getCell(10);
            Cell cell4 = firstSheet.getRow(jobId).getCell(11);
            p = builder1.start();
            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            message="Exporting Data...";
            while (true) {
                line = r.readLine();
                if (line == null) { break; }
                System.out.println(line);
                if(line.contains("exported")){
                    message=message+"...."+line;
                   // cell.setCellValue(line);
                }else if(line.contains("unsuccessfully") || line.contains("unknown parameter name") || line.contains("ORA-")){
                    message=message+"...."+line;
                    //cell.setCellValue(line);
                    cell3.setCellValue("Failed");
                    cell4.setCellValue("Import did not happen as export failed");
                }
            }
            cell.setCellValue(message);
            Cell cell2 = firstSheet.getRow(jobId).getCell(8);
            cell2.setCellValue(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            FileOutputStream fileOut = new FileOutputStream(excelFilePath);
            wb.write(fileOut);
            fileOut.close();
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            throw new RuntimeException("Failed when exporting data. Please contact admin", e);

        }
    }

    public void importData(String toSch, String toPwd, String toDB, int jobId, String toSid){
        try{
            wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
            Sheet firstSheet = wb.getSheetAt(0);
            Cell cell3 = firstSheet.getRow(jobId).getCell(11);
            Cell cell = firstSheet.getRow(jobId).getCell(9);
            Process p = null;
            ProcessBuilder builder1 = new ProcessBuilder(oraPath + "\\BIN\\imp", toSch+"/"+toPwd+"@"+toSid, "file="+jobId+".dmp",
                    "full=y", "log="+jobId+"_import.txt", "ignore=y");
            builder1.directory(new File(dmpFilePath));
            builder1.environment().put("ORACLE_HOME", oraPath );
            builder1.environment().put("PATH", "%ORACLE_HOME%\\BIN;%PATH%");
            builder1.redirectErrorStream(true);
            p = builder1.start();
            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            message="Importing Data...";
            while (true) {
                line = r.readLine();
                if (line == null) { break; }
                System.out.println(line);
                if(line.contains("imported")){
                    message=message+"...."+line;
                    //cell3.setCellValue(line);
                    cell.setCellValue("Completed");
                }
                else if(line.contains("failed") || line.contains("Unable to set values for column")|| line.contains("ORA-")){
                    message=message+"...."+line;
               // else if(line.contains("failed")){
               //     cell3.setCellValue(line);
                    cell.setCellValue("Failed");
                }
            }
            cell3.setCellValue(message);
            Cell cell2 = firstSheet.getRow(jobId).getCell(8);
            cell2.setCellValue(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            FileOutputStream fileOut = new FileOutputStream(excelFilePath);
            wb.write(fileOut);
            fileOut.close();
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
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
            logger.error(e.getMessage(),e.getStackTrace());
            throw new RuntimeException("Failed when reading the file.", e);
        }
        return jobDetailsList;
    }

    public List<String> getValues(String key){
       // String value;
        String valueSeperated[];
        List<String> valueList= new ArrayList<>();
        //Properties configProp = new Properties();
        //InputStream in = this.getClass().getClassLoader().getResourceAsStream("application.properties");
        try {
            configProp.load(in);
            value = System.getenv(key);
            valueSeperated=value.split(",");
            for(String s: valueSeperated){
                valueList.add(s);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(),e.getStackTrace());
            throw new RuntimeException("Failed when fetching values from property file.", e);
        }
        return valueList;
    }
    
    public void deleteFile(int jobId){
        File myObj = new File(dmpFilePath+jobId+".dmp");
        if (myObj.delete()) {
            System.out.println("Deleted the dmp file: " + myObj.getName());
            logger.info("Deleted the dmp file: " + myObj.getName());
        } else {
            System.out.println("Failed to delete the file.");
            logger.warn("Failed to delete the file.");
        }
    }

    public void updateFileStatus(int jobId, String status){
        try {
            wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
            Sheet firstSheet = wb.getSheetAt(0);
            Cell cell = firstSheet.getRow(jobId).getCell(9);
            cell.setCellValue(status);
            Cell cell2 = firstSheet.getRow(jobId).getCell(8);
            cell2.setCellValue(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
            FileOutputStream fileOut = new FileOutputStream(excelFilePath);
            wb.write(fileOut);
            fileOut.close();
        } catch (IOException e) {
            logger.error(e.getMessage(),e.getStackTrace());
            throw new RuntimeException("Error when updating file.", e);
        }
    }
}