package com.vulcan.sau.util;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.apache.log4j.Logger;

import jxl.CellView;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.format.UnderlineStyle;
import jxl.write.Label;
import jxl.write.Number;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import jxl.write.biff.RowsExceededException;

public class ExcelWriter {
	private static final Logger logger = Logger.getLogger(ExcelWriter.class);
	public static final int MAX_RECORDS_PER_SPREADSHEET = 60000;

	private WritableWorkbook workbook = null;
	private WritableSheet sheet = null;
	private WritableCellFormat headingFont = null;
	private WritableCellFormat cellFont = null;

	public ExcelWriter(String spreadSheetTitle, String[] columns, String outputFileName) throws IOException, WriteException {
		// Create and initialize the output spreadsheet
		WorkbookSettings wbSettings = new WorkbookSettings();
		wbSettings.setLocale(new Locale("en", "EN"));
		
		workbook = Workbook.createWorkbook(new File(outputFileName), wbSettings);
		sheet = workbook.createSheet(spreadSheetTitle, 0);

		// Setup fonts for heading and data cells. Also, write out the global heading to the output spreadsheet
		setupFonts();
		createHeading(columns);
	}

	private void setupFonts() throws WriteException {
		// Lets create a font for general data cells
		WritableFont times10pt = new WritableFont(WritableFont.TIMES, 10);
		cellFont = new WritableCellFormat(times10pt);               // Define the cell format
		cellFont.setWrap(true);                                     // Lets automatically wrap the cells

		// create create a bold font with underlines
		//WritableFont times12ptBoldUnderline = new WritableFont(WritableFont.TIMES, 12, WritableFont.BOLD, false, UnderlineStyle.SINGLE);
		WritableFont times12ptBoldUnderline = new WritableFont(WritableFont.TIMES, 12, WritableFont.BOLD, false, UnderlineStyle.NO_UNDERLINE);
		headingFont = new WritableCellFormat(times12ptBoldUnderline);
		headingFont.setWrap(true);                                  // Lets automatically wrap the cells

		CellView cv = new CellView();
		cv.setFormat(cellFont);
		cv.setFormat(headingFont);
		cv.setAutosize(true);
	}

	public void createHeading(String[] columns) throws WriteException {
		for (int i = 0; i < columns.length; i++) {
			addHeaderCell(i, 0, columns[i]);
		}
	}

	public void addHeaderCell(int column, int row, String s) throws RowsExceededException, WriteException {
		Label label;
		label = new Label(column, row, s, headingFont);
		sheet.addCell(label);
	}

	public void addNumberCell(int column, int row, Integer integer) throws WriteException, RowsExceededException {
		Number number;
		number = new Number(column, row, integer, cellFont);
		sheet.addCell(number);
	}

	public void addTextCell(int column, int row, String s) throws WriteException, RowsExceededException {
		Label label;
		label = new Label(column, row, s, cellFont);
		sheet.addCell(label);
	}
	
	public void close() throws IOException, WriteException {
		workbook.write();
		workbook.close();
	}
}
