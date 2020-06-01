package com.example.demo;

import com.example.model.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import oracle.jdbc.OracleCallableStatement;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


@Controller
@Component
public class CopyController {
    String key = "Mary has one cat";
    File inputFile;
    File encryptedFile;
    File decryptedFile;
    private static final Logger logger = LoggerFactory.getLogger(CopyController.class);
    Connection conFromDb, conToDb, connThrough;
    String fromUser, toUser, connThroughUser, connectThroughDb, fromSid, toSid;
    int jobIdforSyn;

    String dctPath = System.getenv("DCT_HOME") + "\\Files\\";
  
    @Autowired
    CopyService copyService;

    
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public ModelAndView index(){
        System.out.println("Reached index!");
        logger.info("Reached index start.");
        ModelAndView mv = new ModelAndView();
        mv.setViewName("index");
        return mv;
    } 
    
    @GetMapping(value = "/getAllData", produces = "application/json")
    public ResponseEntity<List<JobDetails>> getAllJobDetails() {
        List<JobDetails> jobDetailsList = new ArrayList<JobDetails>();
        try {
            jobDetailsList = copyService.readFromFile();
            return ResponseEntity.status(HttpStatus.OK).body(jobDetailsList);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<JobDetails>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    @PostMapping(path = "/copyData", consumes = "application/json"/*, produces = "application/json"*/)
    public ResponseEntity<String> copy(@RequestBody JobDetails jobDetails) throws SQLException {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now;
        now = LocalDateTime.now();
        System.out.println("Start time - " + dtf.format(now));
        logger.info("Start time - " + dtf.format(now));
        int jobId;
        try {
            jobId = copyService.writeToFile(jobDetails.getFromDB(), jobDetails.getFromSchName(), jobDetails.getToDB(), jobDetails.getToSchName(), jobDetails.getTableName(), jobDetails.getCopyType());
            inputFile = new File(dctPath +jobId+".dmp");
            encryptedFile = new File(dctPath +jobId+".encrypted");
            //decryptedFile = new File(dctPath +jobId+".decrypted");
            copyService.export(jobDetails.getFromSchName(), jobDetails.getFromPWD(), jobDetails.getFromDB(), jobDetails.getTableName(), jobId, jobDetails.getCopyType(), jobDetails.getPartition(), jobDetails.getTextArea(), fromSid, conFromDb);
            /*CryptoUtils.encrypt(key, inputFile, encryptedFile);
            CryptoUtils.decrypt(key, encryptedFile, decryptedFile);*/
            copyService.importData(jobDetails.getToSchName(), jobDetails.getToPWD(), jobDetails.getToDB(), jobId, toSid, jobDetails.getCopyType(), jobDetails.getFromSchName());
            CryptoUtils.encrypt(key, inputFile, encryptedFile);
            copyService.deleteFile(jobId);
            now = LocalDateTime.now();
            System.out.println("End time - " + dtf.format(now));
            return ResponseEntity.status(HttpStatus.OK).body("true");
        } catch (CryptoException e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ExceptionUtils.getStackTrace(e));
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ExceptionUtils.getStackTrace(e));
        } finally {
            conFromDb.close();
            conToDb.close();
        }
    }
    
    @GetMapping(value = "/getSrcDBName", produces = "application/json")
    public ResponseEntity<List<String>> getSrcDBName() {
        /* List<String> js1 = new ArrayList<String>();
        try {
            js1.add("UAT");
            js1.add("INT");
            js1.add("BAT");        
            return ResponseEntity.status(HttpStatus.OK).body(js1);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        } */
        List<String> js1 = new ArrayList<String>();
        try {
            js1 = copyService.getValues("DCT_DBNAMES");
            return ResponseEntity.status(HttpStatus.OK).body(js1);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping(value = "/getSchName", produces = "application/json")
    public ResponseEntity<List<String>> getAllDBSchemas() {
        List<String> schemaList = new ArrayList<String>();
        try {
            schemaList = copyService.getValues("DCT_SCHEMAS");
            return ResponseEntity.status(HttpStatus.OK).body(schemaList);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
       /*  List<String> schemaList = new ArrayList<String>();
        try{
            schemaList.add("TRD");
            schemaList.add("COVID");
            schemaList.add("HACK");
            return ResponseEntity.status(HttpStatus.OK).body(schemaList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        } */
    }

    @GetMapping(value = "/getTabName", produces = "application/json")
    public ResponseEntity<List<String>> getAllTables() {
        List<String> tabList = new ArrayList<String>();        
        try {
            Statement st = conFromDb.createStatement();
            ResultSet rs = st.executeQuery("select table_name from all_tables where owner='" + fromUser.toUpperCase() + "'");
            while (rs.next()) {
                tabList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(tabList);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
       
        /* try{
            tabList.add("Customers");
            tabList.add("Trades");
            tabList.add("Job_Details");
            tabList.add("Transactions");
            tabList.add("Trades_10m");
            return ResponseEntity.status(HttpStatus.OK).body(tabList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        } */
    }

    @GetMapping(value = "/getTabName/{usr}", produces = "application/json")
    public ResponseEntity<List<String>> getAllTables(@PathVariable("usr") String usr) {
        List<String> tabList = new ArrayList<String>();
        try {
            Statement st = connThrough.createStatement();
            ResultSet rs = st.executeQuery("select table_name from all_tables where owner='" + usr.toUpperCase() + "'");
            while (rs.next()) {
                tabList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(tabList);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
  /*       try{
            tabList.add("Customers");
            tabList.add("Trades");
            tabList.add("Job_Details");
            tabList.add("Transactions");
            tabList.add("Trades_10m");
            return ResponseEntity.status(HttpStatus.OK).body(tabList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        } */
    }

    @RequestMapping(value = "/getPartName/{table}", method = RequestMethod.GET)
	public ResponseEntity<List<String>> getAllParts(@PathVariable("table") String table) {
        List<String> partList = new ArrayList<String>();
        try {
            Statement st = conFromDb.createStatement();
            ResultSet rs = st.executeQuery("select distinct partition_name from all_tab_partitions where table_name='" + table.toUpperCase() + "' and table_owner='" + fromUser.toUpperCase() + "'");
            while (rs.next()) {
                partList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(partList);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        /* try{
            partList.add(table.toString() + "P1");
            partList.add(table.toString() + "P2");
            partList.add(table.toString() + "P3");
            partList.add(table.toString() + "P4");
            partList.add(table.toString() + "P5");
            return ResponseEntity.status(HttpStatus.OK).body(partList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        } */

        
    }
    @RequestMapping(value = "/getSubPartName/{table}/{partitionName}", method = RequestMethod.GET)
    public ResponseEntity<List<String>> getAllSubParts(@PathVariable("table") String table, @PathVariable("partitionName") String partitionName) {
        List<String> subPartList = new ArrayList<String>();
        try {
            Statement st = conFromDb.createStatement();
            ResultSet rs = st.executeQuery("select distinct subpartition_name from all_tab_subpartitions where table_name='" + table.toUpperCase() + "' and table_owner='" + fromUser.toUpperCase() +"' and partition_NAME='" + partitionName.toUpperCase() + "'");
            while (rs.next()) {
                subPartList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(subPartList);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
   
    @RequestMapping(value = "/authDB", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> authDB(@RequestParam("usr") String user, @RequestParam("pass") String pass,
                                        @RequestParam("dbn") String dbn, @RequestParam("DbType") String dbType)  throws SQLException {
       /*  try{
            //The check DB  Authentication here.
            
            if(user.isEmpty() || pass.isEmpty() || dbn.isEmpty()){
                return ResponseEntity.status(HttpStatus.OK).body("false");                
            }
            if (pass.equals((user.toString() + "123"))){
                return ResponseEntity.status(HttpStatus.OK).body("true");
            }
            
        } catch (Exception e){
            return ResponseEntity.status(HttpStatus.OK).body(ExceptionUtils.getStackTrace(e));
        }
        System.out.println("DB Authentication!" + user + " " + pass + " " + dbn + " " +  (user + "123"));
 */
        
        try {
            String url = "jdbc:oracle:thin:@localhost:1521:" + dbn;
            List<String> tableNamesList = null;
            Class.forName("oracle.jdbc.driver.OracleDriver");
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            if (dbType.equalsIgnoreCase("source")) {
                fromUser = user;
                conFromDb = DriverManager.getConnection(url, user, pass);
                Statement st = conFromDb.createStatement();
                ResultSet rs = st.executeQuery("select name from V$database");
                while (rs.next()) {
                    fromSid=rs.getString(1);
                }
            } else if (dbType.equalsIgnoreCase("target")) {
                toUser=user;
                conToDb = DriverManager.getConnection(url, user, pass);
                Statement st = conToDb.createStatement();
                ResultSet rs = st.executeQuery("select name from V$database");
                while (rs.next()) {
                    toSid=rs.getString(1);
                }
            } else if (dbType.equalsIgnoreCase("user")) {
                connectThroughDb = dbn;
                connThrough = DriverManager.getConnection(url, user, pass);
                connThroughUser = user.substring(user.indexOf("[") + 1).split("]")[0];
            }
            System.out.println("DB Authentication!" + user + " " + pass + " " + dbn);
            return ResponseEntity.status(HttpStatus.OK).body("true");
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ExceptionUtils.getStackTrace(e));
        }
    }    
    
    @RequestMapping(value = "/getAllGrantSchemaList", method = RequestMethod.GET)
    public ResponseEntity<List<String>> getAllGrantSchemaList() {
        List<String> grantedSchemaList = new ArrayList<String>();
        try {
            Statement st = connThrough.createStatement();
            ResultSet rs = st.executeQuery("select distinct grantor from all_tab_privs where grantee='" + connThroughUser.toUpperCase() + "'");            
            grantedSchemaList.add(connThroughUser.toUpperCase());
            while (rs.next()) {    
            grantedSchemaList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(grantedSchemaList);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }

       /*  try{
            grantedSchemaList.add("TRD1");
            grantedSchemaList.add("COVID1");
            grantedSchemaList.add("HACK1");
            return ResponseEntity.status(HttpStatus.OK).body(grantedSchemaList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        } */
    }
    
    @GetMapping(value = "/getColumnName/{usr}/{table}", produces = "application/json")
    public ResponseEntity<List<String>> getAllColumns(@PathVariable("usr") String usr, @PathVariable("table") String table) {
        List<String> columnList = new ArrayList<String>();
        try {
            Statement st = connThrough.createStatement();
            ResultSet rs = st.executeQuery("SELECT column_name FROM all_tab_cols WHERE owner ='" + usr.toUpperCase() + "' and table_name = '" + table.toUpperCase() + "'");
            while (rs.next()) {
                columnList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(columnList);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
       /*  try{
            columnList.add("COL1");
            columnList.add("COL2");
            columnList.add("COL3");
            return ResponseEntity.status(HttpStatus.OK).body(columnList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        } */

    }

    @RequestMapping(value = "/createSyntheticData", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> createSyntheticData(@RequestBody String jsonString) {
        System.out.println("Entered createSyntheticData");
        logger.info("Entered createSyntheticData");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now;
        now = LocalDateTime.now();
        System.out.println("Start time - " + dtf.format(now));
        List<SyntheticCriteria> syntheticCriteriaList = new ArrayList<SyntheticCriteria>();
        List<SyntheticJoins> syntheticJoinsList = new ArrayList<SyntheticJoins>();
        OracleCallableStatement callStmt = null;
        STRUCT structProject1;
        int count1 = 0, count2 = 0;
        try {
            callStmt = (OracleCallableStatement) connThrough.prepareCall("{call PKG_SYNTHETIC_DATA.create_syn_data(?,?,?)}");
             jobIdforSyn = copyService.writeToFile(connectThroughDb, connThroughUser, "NA", "NA", "NA", "SDC");
             ObjectMapper objectMapper = new ObjectMapper();
             ObjectMapper objectMapper2 = new ObjectMapper();
             objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);             
             JsonNode rootNode = objectMapper.readTree(jsonString);
             JsonNode dataWrapNode = rootNode.path("SyntheticDataWrapper");
             JsonNode synJoinNode = dataWrapNode.path("SyntheticJoins");
             JsonNode synDataCreationCount = dataWrapNode.path("synDataCreationCount");
             System.out.println(synDataCreationCount);
             Iterator<JsonNode> elements = synJoinNode.elements();
             while (elements.hasNext()) {
                    JsonNode synJoin = elements.next();             
                    SyntheticJoins sj = objectMapper2.readValue(synJoin.toString(), SyntheticJoins.class);
                    syntheticJoinsList.add(sj);
            }
            STRUCT[] structArrayOfProjects1 = new STRUCT[syntheticJoinsList.size()];
            for(SyntheticJoins sj: syntheticJoinsList){
                Object[] obj = new Object[]{sj.getSchema1(), sj.getTable1(), sj.getColumn1(), sj.isStatic1(), sj.getMaxCount1(),
                        sj.getSchema2(), sj.getTable2(), sj.getColumn2(), sj.isStatic2(), sj.getMaxCount2()};
                StructDescriptor projectTypeDesc = StructDescriptor.createDescriptor("SYNTHETIC_JOIN", connThrough);
                structProject1 = new STRUCT(projectTypeDesc, connThrough, obj);
                structArrayOfProjects1[count1] = structProject1;
                count1++;
            }
             JsonNode synCriteriaNode = dataWrapNode.path("SyntheticCriteria");   
             Iterator<JsonNode> elements2 = synCriteriaNode.elements();             
             while (elements2.hasNext()) {
                    JsonNode synCr = elements2.next();
                    SyntheticCriteria sc = objectMapper2.readValue(synCr.toString(), SyntheticCriteria.class);
                    syntheticCriteriaList.add(sc);
                }
                STRUCT[] structArrayOfProjects2 = new STRUCT[syntheticCriteriaList.size()];
                for(SyntheticCriteria sc: syntheticCriteriaList){
                    Object[] obj = new Object[]{sc.getSchema(), sc.getTable(), sc.getColumn(), sc.getCondition(), sc.getValue()};
                    StructDescriptor projectTypeDesc = StructDescriptor.createDescriptor("SYNTHETIC_CRITERIA", connThrough);
                    structProject1 = new STRUCT(projectTypeDesc, connThrough, obj);
                    structArrayOfProjects2[count2] = structProject1;
                    count2++;
                }
                ArrayDescriptor projectTypeArrayDesc1 = ArrayDescriptor.createDescriptor("SYNJOINARR", connThrough);
                ArrayDescriptor projectTypeArrayDesc2 = ArrayDescriptor.createDescriptor("SYNCRIARR", connThrough);
                ARRAY arrayOfProjects1 = new ARRAY(projectTypeArrayDesc1, connThrough, structArrayOfProjects1);
                ARRAY arrayOfProjects2 = new ARRAY(projectTypeArrayDesc2, connThrough, structArrayOfProjects2);
                callStmt.setARRAY(1, arrayOfProjects1
                );
                callStmt.setARRAY(2, arrayOfProjects2);
                callStmt.setInt(3, Integer.parseInt(String.valueOf(synDataCreationCount)));
                System.out.println("Calling procedure");
                callStmt.execute();
                System.out.println("Procedure call completed");
                now = LocalDateTime.now();
                System.out.println("End time - " + dtf.format(now));
                logger.info("End time - " + dtf.format(now));
                copyService.updateFileStatus(jobIdforSyn, "Completed");
                return ResponseEntity.status(HttpStatus.OK).body("true");
            } catch (Exception e) {
                copyService.updateFileStatus(jobIdforSyn, "Failed");
                logger.error(e.getMessage(),e.getStackTrace());
                return (ResponseEntity<String>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
            }
    }

    @GetMapping(value = "/getLengthAndType/{table}/{column}", produces = "application/json")
    public ResponseEntity<List<String>> getLength(@PathVariable("table") String table, @PathVariable("column") String column) {
        List<String> colDetails = new ArrayList<String>();
        try {
            Statement st = connThrough.createStatement();
            ResultSet rs = st.executeQuery("select data_length from user_tab_columns where table_name = '"+table.toUpperCase()+"' and column_name='"+column.toUpperCase()+"'");
            while (rs.next()) {
                colDetails.add(String.valueOf(rs.getInt(1)));
                System.out.println("Column Length - > "+String.valueOf(rs.getInt(1)));
            }
            rs = st.executeQuery("select data_type from user_tab_columns where table_name = '"+table.toUpperCase()+"' and column_name='"+column.toUpperCase()+"'");
            while (rs.next()) {
                colDetails.add(rs.getString(1));
                System.out.println("Column Type - > "+rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(colDetails);
        } catch (Exception e) {
            logger.error(e.getMessage(),e.getStackTrace());
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}