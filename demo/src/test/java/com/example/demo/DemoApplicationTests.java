package com.example.demo;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DemoApplicationTests {

	@Autowired
	private CopyService copyService;

	String excelFilePath = "D:\\Vishakha\\Job_Details.xlsx";
	Workbook wb;

	@Test
	public void positiveWriteToFileTest() throws IOException {
		wb = WorkbookFactory.create(new FileInputStream(excelFilePath));
		Sheet sheet = wb.getSheetAt(0);
		int originalCount=sheet.getLastRowNum();
		//copyService.export("trd","trd","iedb_uat", "trades", 20, "TC", "", "");
		int jobId = copyService.writeToFile("iedb_uat","trd", "iedb_dev", "trd", "trades", "TC");
		FileOutputStream fileOut = new FileOutputStream(excelFilePath);
		wb.write(fileOut);
		fileOut.close();
		assertTrue(jobId==originalCount+1);
	}

	@Test(expected = FileNotFoundException.class)
	public void negativeWriteToFileTest() throws IOException {
		wb = WorkbookFactory.create(new FileInputStream("C:\\Files.xlsx"));
		Sheet sheet = wb.getSheetAt(0);
		int originalCount=sheet.getLastRowNum();
		//copyService.export("trd","trd","iedb_uat", "trades", 20, "TC", "", "");
		int jobId = copyService.writeToFile("iedb_uat","trd", "iedb_dev", "trd", "trades", "TC");
		FileOutputStream fileOut = new FileOutputStream(excelFilePath);
		wb.write(fileOut);
		fileOut.close();
	}

	@Test
	public void positiveReadFromFileTest() throws ParseException {
		JobDetails jobDetails = (JobDetails) copyService.readFromFile();
		assertNotNull(jobDetails);
	}

	@Test(expected = FileNotFoundException.class)
	public void negativeReadFromFileTest() throws IOException, ParseException {
		wb = WorkbookFactory.create(new FileInputStream("C:\\Files.xlsx"));
		copyService.readFromFile();
	}
}
