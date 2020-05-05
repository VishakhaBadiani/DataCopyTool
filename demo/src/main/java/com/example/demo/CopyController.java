package com.example.demo;

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
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import java.io.File;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RestController
@Component
public class CopyController {
    String key = "Mary has one cat";
    File inputFile;
    File encryptedFile;
    File decryptedFile;

    Connection conFromDb, conToDb, connThrough;
    String fromUser, connThroughUser, connectThroughDb;

    @Autowired
    CopyService copyService;

    @RequestMapping("/login")
    public ModelAndView firstPage() {
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
            return (ResponseEntity<List<JobDetails>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(path = "/copyData", consumes = "application/json"/*, produces = "application/json"*/)
    public ResponseEntity<String> copy(@RequestBody JobDetails jobDetails) throws SQLException {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now;
        now = LocalDateTime.now();
        System.out.println("Start time - " + dtf.format(now));
        int jobId;
        try {
            jobId = copyService.writeToFile(jobDetails.getFromDB(), jobDetails.getFromSchName(), jobDetails.getToDB(), jobDetails.getToSchName(), jobDetails.getTableName(), jobDetails.getCopyType());
            inputFile = new File("D:\\Vishakha\\Files\\" + jobId + ".dmp");
            encryptedFile = new File("D:\\Vishakha\\Files\\" + jobId + ".encrypted");
            decryptedFile = new File("D:\\Vishakha\\Files\\" + jobId + ".decrypted");
            copyService.export(jobDetails.getFromSchName(), jobDetails.getFromPWD(), jobDetails.getFromDB(), jobDetails.getTableName(), jobId, jobDetails.getCopyType(), jobDetails.getPartition(), jobDetails.getTextArea());
            /*CryptoUtils.encrypt(key, inputFile, encryptedFile);
            CryptoUtils.decrypt(key, encryptedFile, decryptedFile);*/
            copyService.importData(jobDetails.getToSchName(), jobDetails.getToPWD(), jobDetails.getToDB(), jobId);
            CryptoUtils.encrypt(key, inputFile, encryptedFile);
            copyService.deleteFile(jobId);
            now = LocalDateTime.now();
            System.out.println("End time - " + dtf.format(now));
            return ResponseEntity.status(HttpStatus.OK).body("true");
        } catch (CryptoException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ExceptionUtils.getStackTrace(e));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ExceptionUtils.getStackTrace(e));
        } finally {
            conFromDb.close();
            conToDb.close();
        }
    }

    @GetMapping(value = "/getSrcDBName", produces = "application/json")
    public ResponseEntity<List<String>> getSrcDBName() {
        List<String> js1 = new ArrayList<String>();
        try {
            js1 = copyService.getValues("dbNames");
            return ResponseEntity.status(HttpStatus.OK).body(js1);
        } catch (Exception e) {
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping(value = "/getSchName", produces = "application/json")
    public ResponseEntity<List<String>> getAllDBSchemas() {
        List<String> schemaList = new ArrayList<String>();
        try {
            schemaList = copyService.getValues("schemaNames");
            return ResponseEntity.status(HttpStatus.OK).body(schemaList);
        } catch (Exception e) {
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
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
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
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
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
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
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/authDB", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> authDB(@RequestParam("usr") String user, @RequestParam("pass") String pass,
                                         @RequestParam("dbn") String dbn, @RequestParam("DbType") String dbType) throws SQLException {
        try {
            String url = "jdbc:oracle:thin:@localhost:1521:" + dbn;
            List<String> tableNamesList = null;
            Class.forName("oracle.jdbc.driver.OracleDriver");
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            if (dbType.equalsIgnoreCase("source")) {
                fromUser = user;
                conFromDb = DriverManager.getConnection(url, user, pass);
            } else if (dbType.equalsIgnoreCase("target")) {
                conToDb = DriverManager.getConnection(url, user, pass);
            } else if (dbType.equalsIgnoreCase("user")) {
                connectThroughDb = dbn;
                connThrough = DriverManager.getConnection(url, user, pass);
                connThroughUser = user.substring(user.indexOf("[") + 1).split("]")[0];
                /*CallableStatement cStmt = connThrough.prepareCall("{call greetings()}");
                cStmt.executeQuery();*/
                /*CallableStatement stmnt = connThrough.prepareCall("call customProc(?)");
                ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("PARAM_HOLDER_OBJ_TABLE", connThrough);
                ParamHolder[] paramArray = new ParamHolder[]{                                          //create an array of four elements
                        new ParamHolder("param1","param2","param3"),
                        new ParamHolder("param1","param2","param3"),
                        new ParamHolder("param1","param2","param3"),
                        new ParamHolderf("param1","param2","param3")                                                   //Each array element represents a set of InputParams
                };
                Array array = new ARRAY(descriptor , connThrough, paramArray);
                stmnt.setArray(1, array );
                boolean value = stmnt.execute();
                System.out.println(value);*/
            }
            System.out.println("DB Authentication!" + user + " " + pass + " " + dbn);
            return ResponseEntity.status(HttpStatus.OK).body("true");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ExceptionUtils.getStackTrace(e));
        }
    }

    @RequestMapping(value = "/getAllGrantSchemaList", method = RequestMethod.GET)
    public ResponseEntity<List<String>> getAllGrantSchemaList() {
        List<String> grantedSchemaList = new ArrayList<String>();
        try {
            Statement st = connThrough.createStatement();
            ResultSet rs = st.executeQuery("select distinct grantor from all_tab_privs where grantee='" + connThroughUser.toUpperCase() + "'");
            /*ResultSet rs1 = st.executeQuery("DELETE FROM PLAN_TABLE");
            int rs2 = st.executeUpdate("BEGIN EXECUTE IMMEDIATE 'select * from job_details'; END;");
            String sql = "DELETE FROM PLAN_TABLE; " +
                    " BEGIN EXECUTE IMMEDIATE 'select * from job_details'; END; " +
                    " SELECT    'SELECT DISTINCT ' || OBJECT_ALIAS || '.* FROM ' || (SELECT LISTAGG (ojn.OBJECT_NAME || ' ' || ojn.OBJECT_ALIAS, ', ') WITHIN GROUP (ORDER BY ojn.OBJECT_NAME || ' ' || ojn.OBJECT_ALIAS) FROM OBJ_NAME ojn)\n" +
                    "       || CASE WHEN (SELECT DISTINCT LISTAGG (REPLACE (NVL (ACCESS_PREDICATES, FILTER_PREDICATES),'\"',NULL), ' AND ') WITHIN GROUP (ORDER BY NVL (ACCESS_PREDICATES, FILTER_PREDICATES)) FROM PLAN_TABLE\n" +
                    "                     WHERE NVL (ACCESS_PREDICATES, FILTER_PREDICATES) IS NOT NULL) IS NOT NULL\n" +
                    "              THEN\n" +
                    "                     ' WHERE ' || REPLACE((SELECT DISTINCT LISTAGG (REPLACE ( NVL (ACCESS_PREDICATES, FILTER_PREDICATES), '\"', NULL), ' AND ') WITHIN GROUP (ORDER BY NVL (ACCESS_PREDICATES, FILTER_PREDICATES))\n" +
                    "                        FROM PLAN_TABLE WHERE NVL (ACCESS_PREDICATES, FILTER_PREDICATES) IS NOT NULL),'INTERNAL_FUNCTION',NULL)\n" +
                    "          END || ';'    QUERY1\n" +
                    "  FROM (SELECT DISTINCT P.OBJECT_NAME,\n" +
                    "                    REPLACE (REGEXP_SUBSTR (OBJECT_ALIAS,'([^@]+)',1,1,NULL,1),'\"',NULL)    OBJECT_ALIAS,\n" +
                    "                    ROWNUM RN\n" +
                    "      FROM PLAN_TABLE P\n" +
                    "     WHERE P.OBJECT_NAME IS NOT NULL AND P.OBJECT_NAME NOT LIKE 'SYS%' AND OBJECT_OWNER <> 'SYS')  OJ,\n" +
                    "       (    SELECT LEVEL     LVL\n" +
                    "              FROM DUAL\n" +
                    "        CONNECT BY LEVEL <= (SELECT COUNT (*) FROM (SELECT DISTINCT P.OBJECT_NAME,\n" +
                    "                    REPLACE (REGEXP_SUBSTR (OBJECT_ALIAS,'([^@]+)',1,1,NULL,1),'\"',NULL)    OBJECT_ALIAS,\n" +
                    "                    ROWNUM RN\n" +
                    "      FROM PLAN_TABLE P\n" +
                    "     WHERE P.OBJECT_NAME IS NOT NULL AND P.OBJECT_NAME NOT LIKE 'SYS%' AND OBJECT_OWNER <> 'SYS'))) X\n" +
                    " WHERE OJ.RN = X.LVL";
            ResultSet rs3 = st.executeQuery(sql);
            while(rs3.next()){
                System.out.println(rs3.next());
            }*/
            grantedSchemaList.add(connThroughUser.toUpperCase());
            while (rs.next()) {
                grantedSchemaList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(grantedSchemaList);
        } catch (Exception e) {
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
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
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/createSyntheticData", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> createSyntheticData(@RequestBody String jsonString) {
        System.out.println("Entered createSyntheticData");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now;
        now = LocalDateTime.now();
        System.out.println("Start time - " + dtf.format(now));
        List<SyntheticCriteria> syntheticCriteria = new ArrayList<SyntheticCriteria>();
        List<SyntheticJoins> syntheticJoins = new ArrayList<SyntheticJoins>();
        OracleCallableStatement callStmt = null;
        STRUCT structProject1;
        STRUCT[] structArrayOfProjects1 = new STRUCT[10];
        STRUCT[] structArrayOfProjects2 = new STRUCT[10];
        int count1 = 0, count2 = 0;
        try {
            callStmt = (OracleCallableStatement) connThrough.prepareCall("{call create_syn_data(?,?)}");
            int jobId = copyService.writeToFile(connectThroughDb, connThroughUser, "NA", "NA", "NA", "SDC");
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectMapper objectMapper2 = new ObjectMapper();
            objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            JsonNode rootNode = objectMapper.readTree(jsonString);
            JsonNode dataWrapNode = rootNode.path("SyntheticDataWrapper");
            JsonNode synJoinNode = dataWrapNode.path("SyntheticJoins");
            Iterator<JsonNode> elements = synJoinNode.elements();
            while (elements.hasNext()) {
                JsonNode synJoin = elements.next();
                SyntheticJoins sj = objectMapper2.readValue(synJoin.toString(), SyntheticJoins.class);
                Object[] obj = new Object[]{sj.getSchema1(), sj.getTable1(), sj.getColumn1(), sj.getStatic1(), sj.getMaxCount1(),
                        sj.getSchema2(), sj.getTable2(), sj.getColumn2(), sj.getStatic2(), sj.getMaxCount2()};
                StructDescriptor projectTypeDesc = StructDescriptor.createDescriptor("SYNTHETIC_JOIN", connThrough);
                structProject1 = new STRUCT(projectTypeDesc, connThrough, obj);
                structArrayOfProjects1[count1] = structProject1;
                count1++;
                syntheticJoins.add(sj);
            }
            JsonNode synCriteriaNode = dataWrapNode.path("SyntheticCriteria");
            Iterator<JsonNode> elements2 = synCriteriaNode.elements();
            while (elements2.hasNext()) {
                JsonNode synCr = elements2.next();
                SyntheticCriteria sc = objectMapper2.readValue(synCr.toString(), SyntheticCriteria.class);
                Object[] obj = new Object[]{sc.getSchema(), sc.getTab(), sc.getCol(), sc.getCondition(), sc.getValue()};
                StructDescriptor projectTypeDesc = StructDescriptor.createDescriptor("SYNTHETIC_CRITERIA", connThrough);
                structProject1 = new STRUCT(projectTypeDesc, connThrough, obj);
                structArrayOfProjects2[count2] = structProject1;
                count2++;
                syntheticCriteria.add(sc);
            }
            for (SyntheticJoins synJ : syntheticJoins) {
                System.out.println(synJ.toString());
            }
            for (SyntheticCriteria synC : syntheticCriteria) {
                System.out.println(synC.toString());
            }
            ArrayDescriptor projectTypeArrayDesc1 = ArrayDescriptor.createDescriptor("SYNJOINARR", connThrough);
            ArrayDescriptor projectTypeArrayDesc2 = ArrayDescriptor.createDescriptor("SYNCRIARR", connThrough);
            ARRAY arrayOfProjects1 = new ARRAY(projectTypeArrayDesc1, connThrough, structArrayOfProjects1);
            ARRAY arrayOfProjects2 = new ARRAY(projectTypeArrayDesc2, connThrough, structArrayOfProjects2);
            callStmt.setARRAY(1, arrayOfProjects1);
            callStmt.setARRAY(2, arrayOfProjects2);
            callStmt.execute();
            now = LocalDateTime.now();
            System.out.println("End time - " + dtf.format(now));
            copyService.updateFileStatus(jobId, "Completed");
            return ResponseEntity.status(HttpStatus.OK).body("true");
        } catch (Exception e) {
            return (ResponseEntity<String>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/createSyntheticDataStr", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> createSyntheticDataStr(@RequestBody String jsonString) {
        System.out.println("Entered createSyntheticData");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now;
        now = LocalDateTime.now();
        System.out.println("Start time - " + dtf.format(now));
        OracleCallableStatement callStmt = null;
        try {
            callStmt = (OracleCallableStatement) connThrough.prepareCall("{call create_syn_data_single_str(?)}");
            int jobId = copyService.writeToFile(connectThroughDb, connThroughUser, "NA", "NA", "NA", "SDC");
            callStmt.setString(1, jsonString);
            callStmt.execute();
            now = LocalDateTime.now();
            System.out.println("End time - " + dtf.format(now));
            copyService.updateFileStatus(jobId, "Completed");
            return ResponseEntity.status(HttpStatus.OK).body("true");
        } catch (Exception e) {
            return (ResponseEntity<String>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}