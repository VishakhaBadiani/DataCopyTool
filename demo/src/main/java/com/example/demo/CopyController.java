package com.example.demo;

import model.JobDetails;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@RestController
@Component
public class CopyController {
    Connection conFromDb, conToDb;

    @Autowired
    CopyService copyService;

    @RequestMapping("/login")
    public ModelAndView firstPage(){
        ModelAndView mv = new ModelAndView();
        mv.setViewName("index");
        return mv;
    }

    @GetMapping(value="/getAllData", produces = "application/json")
    public ResponseEntity<List<JobDetails>> getAllJobDetails(){
        List<JobDetails> jobDetailsList = new ArrayList<JobDetails>();
        try{
            jobDetailsList=copyService.readFromFile();
            return ResponseEntity.status(HttpStatus.OK).body(jobDetailsList);
        }catch(Exception e){
            return (ResponseEntity<List<JobDetails>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(path = "/copyData", consumes = "application/json"/*, produces = "application/json"*/)
    public ResponseEntity<String> copy(@RequestBody JobDetails jobDetails){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now;
        now=LocalDateTime.now();
        System.out.println("Start time - "+dtf.format(now));
        int jobId;
        try {
            jobId = copyService.writeToFile(jobDetails.getFromDB(), jobDetails.getFromSchName(), jobDetails.getToDB(), jobDetails.getToSchName(), jobDetails.getTableName(), jobDetails.getCopyType());
            copyService.export(jobDetails.getFromSchName(), jobDetails.getFromPWD(), jobDetails.getFromDB(), jobDetails.getTableName(), jobId, jobDetails.getCopyType(), jobDetails.getPartition(), jobDetails.getTextArea());
            conFromDb.close();
            copyService.importData(jobDetails.getToSchName(), jobDetails.getToPWD(), jobDetails.getToDB(), jobId);
            now=LocalDateTime.now();
            System.out.println("End time - "+dtf.format(now));
            return ResponseEntity.status(HttpStatus.OK).body("true");
        }catch (Exception e){
            return ResponseEntity.status(HttpStatus.OK).body(ExceptionUtils.getStackTrace(e));
        }
    }

    @GetMapping(value = "/getSrcDBName", produces = "application/json")
    public ResponseEntity<List<String>> getSrcDBName() {
        List<String> js1 = new ArrayList<String>();
        try {
            js1 = copyService.getValues("dbNames");
            return ResponseEntity.status(HttpStatus.OK).body(js1);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping(value="/getSchName", produces = "application/json")
    public ResponseEntity<List<String>> getAllDBSchemas(){
        List<String> schemaList = new ArrayList<String>();
        try{
            schemaList = copyService.getValues("schemaNames");
            return ResponseEntity.status(HttpStatus.OK).body(schemaList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping(value="/getTabName", produces = "application/json")
    public ResponseEntity<List<String>> getAllTables(@PathVariable("user") String user){
        List<String> tabList = new ArrayList<String>();
        try{
            Statement st = conFromDb.createStatement();
            ResultSet rs = st.executeQuery("select table_name from all_tables where owner='"+user.toUpperCase()+"'");
            while(rs.next()){
                tabList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(tabList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value="/getPartName/{table}", method = RequestMethod.GET)
    public ResponseEntity<List<String>> getAllParts(@PathVariable("table") String table, @PathVariable("user") String user) {
        List<String> partList = new ArrayList<String>();
        try{
            Statement st = conFromDb.createStatement();
            ResultSet rs = st.executeQuery("select distinct partition_name from all_tab_partitions where table_name='"+table.toUpperCase()+"' and table_owner='"+user.toUpperCase()+"'");
            while(rs.next()){
                partList.add(rs.getString(1));
            }
            return ResponseEntity.status(HttpStatus.OK).body(partList);
        }catch(Exception e){
            return (ResponseEntity<List<String>>) ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(path = "/authDB", consumes = "application/text")
    public ResponseEntity<String> authDB(@RequestParam("usr") String user, @RequestParam("pass") String pass,
                                         @RequestParam("dbname") String dbn, @RequestParam("DbType") String dbType) {
        try {
            String url = "jdbc:oracle:thin:@localhost:1521:"+dbn;
            List<String> tableNamesList = null;
            Class.forName("oracle.jdbc.driver.OracleDriver");
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            if(dbType.equalsIgnoreCase("source")){
                conFromDb = DriverManager.getConnection(url,user,pass);
            }else if(dbType.equalsIgnoreCase("target")){
                conToDb = DriverManager.getConnection(url,user,pass);
            }
            System.out.println("DB Authentication!" + user + " " + pass + " " + dbn);
            return ResponseEntity.status(HttpStatus.OK).body("true");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ExceptionUtils.getStackTrace(e));
        }
    }
}