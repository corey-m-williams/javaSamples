package com.cwilliams.commerce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cwilliams.commerce.db.SMDBPool;
import com.cwilliams.commerce.mail.SMMailer;

import com.cwilliams.commerce.ProductView;

public class AccessLoggerQueue {
	private static int CHUNK_SIZE = 50;
	private static String ERROR_FILE = "/tmp/entitlementErrors";
	private static String ERROR_EMAIL = "errors@cwilliams.com";
	
	private static BlockingQueue<ProductView> accessQueue;
	
	protected volatile static Thread queueHandler;
	private volatile static boolean errorEmailSent = false;
	
	private final static Log logger = LogFactory.getLog(AccessLoggerQueue.class);
	
	/**
	 * Primarily for testing, to make sure that the queue is being processed properly
	 * @return
	 */
	protected static boolean isEmpty(){
		return accessQueue.isEmpty();
	}
	protected static int getSize(){
		return accessQueue.size();
	}
	
	protected static void writeIpLogEntry(Connection conn, ProductView view) throws SQLException{
		final String insertIpLogSql = "INSERT INTO IP_ACCESSES (IP_ADDRESS, USER_ID, TSTAMP) VALUES (?, ?, ?)";
		PreparedStatement stmt = conn.prepareStatement(insertIpLogSql);
		stmt.setString(1, view.getIpAddress());
		stmt.setInt(2, view.getUserID());
		stmt.setTimestamp(3, new Timestamp(view.getDebitTime()));
		stmt.execute();
		stmt.close();
	}
	/**
	 * Writes the provided view to the access log via the DB conection conn
	 * @param conn
	 * @param view
	 */
	protected static void writeEntLogEntry(Connection conn, ProductView view) throws SQLException{
		System.out.format("Writing access log entry for productView: %s\n", view);
		final String insertEntsLogSql = "INSERT INTO ENTITLEMENTS_LOG (TSTAMP, USER_ID, ENTITLEMENT_ID, PRODUCT_ID, DATA) VALUES (?, ?, ?, ?, ?)";
		PreparedStatement stmt = conn.prepareStatement(insertEntsLogSql);
		
		stmt.setTimestamp(1, new Timestamp(view.getDebitTime()));
		stmt.setInt(2, view.getUserID());
		stmt.setLong(3, view.getEntitlementID());
		stmt.setInt(4, view.getProductID());
		stmt.setString(5, view.getDataAsCSV());
		stmt.execute();
		stmt.close();
	}
	
	/**
	 * Opens either the passed in errorFile, or a temporary file, and returns that file
	 *   as long as it opens properly and is writable.
	 * @param errorFile
	 * @return The error file
	 */
	private static File openErrorFile(String errorFilePath, boolean writeCheck) throws IllegalArgumentException{
		File errorFile = null;
		try{
			errorFile = new File(errorFilePath);
		}catch (Exception e) {
			try{
				logger.error(String.format("Unable to create Error file for AccessLoggerQueue at %s)", errorFilePath));
				errorFile = File.createTempFile("accessLoggerQueue", ".views");
				logger.error(String.format("AccessLoggerQueue Error Temp File: %s", errorFile.getAbsolutePath()));
			}catch(Exception e2){
				logger.error("AccessLoggerQueue is unable to create temporary error file. Giving Up!");
			}
		}
		if(errorFile != null){
			if(writeCheck){
				if(!errorFile.exists() || errorFile.canWrite()){
					return errorFile;
				}else{
					logger.error(String.format("File: %s is not able to be written to!, unable to log errors", errorFile.getAbsolutePath()));
					throw new IllegalArgumentException(String.format("Unable to write to error file!: %s", errorFilePath));
				}
			}else{
				if(errorFile.exists()){
					return errorFile;
				}else{
					throw new IllegalArgumentException(String.format("Unable to read error file: %s", errorFilePath));
				}
			}
		}
		return errorFile;
	}
	
	protected static String generateErrorMessageComment(Throwable error){
		StringBuilder ret = new StringBuilder();
		
		ret.append('#').append(error.getMessage()).append("\n");
		
		//Add a comment to each line of the stack trace
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		error.printStackTrace(pw);
		String[] stackTraceLines = sw.toString().split("\n");
		for(int i = 0; i < stackTraceLines.length; i++){
			ret.append('#').append(stackTraceLines[i]).append("\n");
		}
		
		return ret.toString();
	}
	
	/**
	 * Writes the provided list of ProductViews to the errorFile, making sure to flush the file afterwards
	 * In case of not being able to open/write to the errorFile, create a temporary file and write to that.
	 *   The temporary file's path will be output to the log
	 * @param cause - Cause of the errors
	 * @param views - Current chunk of views
	 * @param errorFile - File path to write errors to
	 */
	protected static void writeViewsToErrorLog(Throwable cause, Vector<ProductView> views, String errorFile){
		File outFile = null;
		FileOutputStream outStream = null;
		try{
			outFile = openErrorFile(errorFile, true);
			outStream = new FileOutputStream(outFile, true);
		}catch(Exception e){
			logger.error(String.format("Unable to Open error file: %s", (outFile != null) ? outFile.getAbsolutePath() : "null"));
		}
		if(outStream != null){
			Iterator<ProductView> viewIter = views.iterator();
			try {
				outStream.write(generateErrorMessageComment(cause).getBytes());
			} catch (IOException e1) {
				logger.error(String.format("Error writing error message to file!"));
				e1.printStackTrace();
			}
			while(viewIter.hasNext()){
				//Write out the cause message/stack trace as a comment (Prefixed with '#')
				ProductView curView = viewIter.next();
				try {
					outStream.write(curView.toCSV().getBytes());
					outStream.flush();
				} catch (IOException e) {
					logger.error(String.format("Error writing a view to file! view: %s", curView.toCSV()));
					e.printStackTrace();
				}
			}
		}
	}
	
	protected static void processViews(Connection conn, Collection<ProductView> views) throws SQLException{
		try{
			conn.setAutoCommit(false);
			Iterator<ProductView> viewIter = views.iterator();
			while(viewIter.hasNext()){
				ProductView curView = viewIter.next();
				writeEntLogEntry(conn, curView);
				writeIpLogEntry(conn, curView);
				decrementEntitlement(conn, curView);
			}
			conn.commit();
			conn.setAutoCommit(true);
			views.clear();
		}catch(SQLException e){
			//Attempt to rollback the connection before re-throwing the exception
			conn.rollback();
			conn.setAutoCommit(true);
			throw e;
		}
	}
	
	/**
	 * Drains up to chunkSize items from the the queue, and sends them to {@link writeLogEntry}, 
	 * upon any error during processing the chunk, it then proceeds to send the current chunk to {@link writeViewsToErrorLog}
	 * @param conn
	 * @param chunkSize
	 */
	protected static void drainQueue(Connection conn, int chunkSize){
		Vector<ProductView> views = new Vector<ProductView>(CHUNK_SIZE);
		int drained = 0;
		
		//Take up to CHUNK_SIZE elements out of the queue and process them
		drained = getAccessQueue().drainTo(views, CHUNK_SIZE);
		if(drained != 0){
			System.out.format("Found some views to log: %d\n", drained);
			try{
				processViews(conn, views);
			}catch(Exception e){
				System.out.format("Error logging view: %s\n", e.getMessage());
				writeViewsToErrorLog(e, views, ERROR_FILE);
			}
		}
	}
	
	
	/**
	 * Chunk of an error file, a combination of the initial comments describing the error, as well
	 *   as the views until the next comment
	 * @author corey
	 *
	 */
	protected static class ErrorFileChunk{
		/**
		 * Vector of comments in file, including the preceding '#'
		 */
		Vector<String> comments;
		Vector<ProductView> views;
		
		public Vector<ProductView> getViews(){
			return views;
		}
		public Vector<String> getComments(){
			return comments;
		}
		
		public boolean isEmpty(){
			return views.isEmpty();
		}
		
		public ErrorFileChunk(){
			comments = new Vector<String>();
			views = new Vector<ProductView>(CHUNK_SIZE);
		}
		
		public static List<ErrorFileChunk> listFromInputReader(BufferedReader in) throws IOException{
			List<ErrorFileChunk> chunks = new ArrayList<ErrorFileChunk>();
			ErrorFileChunk curChunk = ErrorFileChunk.fromInputReader(in);
			//Until we empty ERROR_FILE
			while(!curChunk.isEmpty()){
				//Attempt to load an ErrorFileChunk from the file, add it to our list of chunks
				chunks.add(curChunk);
				curChunk = ErrorFileChunk.fromInputReader(in);
			}
			
			return chunks;
		}
		
		public static ErrorFileChunk fromInputReader(BufferedReader in) throws IOException{
			ErrorFileChunk chunk = new ErrorFileChunk();
			
			boolean readAView = false;
			
			in.mark(512);
			String line = in.readLine();
			while(line != null){
				line = line.trim();
				if(line.startsWith("#")){
					if(readAView){
						in.reset();
						//leave the while loop
						break;
					}else{
						chunk.comments.add(line);
					}
				}else if(!line.isEmpty()){
					readAView = true;
					chunk.views.add(new ProductView(line));
				}
				in.mark(512);
				line = in.readLine();
			}
			
			return chunk;
		}
		
		@Override
		/**
		 * Outputs this ErrorFileChunk as a valid Chunk of an error file
		 */
		public String toString(){
			StringBuilder ret = new StringBuilder();
			
			Iterator<String> commentIter = comments.iterator();
			while(commentIter.hasNext()){
				String curComment = commentIter.next();
				ret.append(curComment).append("\n");
			}
			
			Iterator<ProductView> viewIter = views.iterator();
			while(viewIter.hasNext()){
				ProductView curView = viewIter.next();
				//Csv conversion already adds newline
				ret.append(curView.toCSV());
			}
			return ret.toString();
		}
		
		public static void writeChunksToFile(Collection<ErrorFileChunk> chunks, String filepath) throws IllegalArgumentException, IOException{
			writeChunksToFile(chunks, filepath, true);
		}
		
		public static void writeChunksToFile(Collection<ErrorFileChunk> chunks, String filepath, boolean append) throws IllegalArgumentException, IOException{
			File outFile = openErrorFile(filepath, true);
			if(!append){
				outFile.delete();
			}
			if(outFile != null){
				FileOutputStream fos = new FileOutputStream(outFile);
				Iterator<ErrorFileChunk> chunkIter = chunks.iterator();
				while(chunkIter.hasNext()){
					ErrorFileChunk curChunk = chunkIter.next();
					curChunk.writeToStream(fos);
				}
			}else{
				throw new IOException("Unable to open error file for output");
			}
		}
		
		private void writeToStream(OutputStream out) throws IOException{
			out.write(this.toString().getBytes());
		}
	}
	
	protected static boolean decrementEntitlement(Connection conn, ProductView view){
		String updateSql = "UPDATE ENTITLEMENTS SET UNITS = (UNITS - 1) WHERE ENTITLEMENT_ID = ? AND UNITS > 0 AND USER_ID = ?";
		try {
			PreparedStatement stmt = conn.prepareStatement(updateSql);
			stmt.setLong(1, view.getEntitlementID());
			stmt.setInt(2, view.getUserID());
			stmt.execute();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private static void writeObjectsToLog(Collection<? extends Object> objs){
		Iterator<? extends Object> objIter = objs.iterator();
		while(objIter.hasNext()){
			logger.error(objIter.next().toString());
		}
	}
	
	/**
	 * Attempts to process the views in the provided ErrorFileChunks
	 * Sends an email to ERROR_EMAIL on error, as long as the error email flag is not set
	 * @param chunks
	 * @return the number of chunks that were successfully processed
	 */
	protected static int handleErrorChunks(List<ErrorFileChunk> chunks){
		int i = 0;
		for(i = 0; i < chunks.size(); i++){
			ErrorFileChunk curChunk = chunks.get(i);
			System.out.format("Found a chunk: \n%s", curChunk);
			try{
				processViews(SMDBPool.getInstance().getDataSource().getConnection(), curChunk.getViews());
			}catch(SQLException e){
				if(!isErrorEmailSent()){
					setErrorEmailSent(true);
					SMMailer.sendMessage(ERROR_EMAIL, "Access Logger Queue Error", 
							"There was an error processing the Errors file in the Access Logger Queue\n" +
							"The exception message is:" + e.getMessage() + "\n" +
							"The Error file chunk is as follows: \n" +
							curChunk.toString());
				}
				try {
					logger.error("Unable to process Error File chunks");
					e.printStackTrace();
					ErrorFileChunk.writeChunksToFile(chunks.subList(i, chunks.size()), ERROR_FILE);
					break;
				} catch (IOException e1) {
					logger.error("Unable to write chunks to error file");
					e1.printStackTrace();
					writeObjectsToLog(chunks.subList(i, chunks.size()));
					break;
				}
			}
		}
		return i;
	}
	
	/**
	 * Processes a passed in string of error file chunks
	 * This method will clear the ErrorEmailSent flag if successful
	 * @param chunkText
	 * @return True on successful processing of the chunks
	 * 		   False if there was a failure
	 */
	public static boolean handleErrorChunksString(String chunkText){
		StringReader sr = new StringReader(chunkText);
		BufferedReader br = new BufferedReader(sr);
		List<ErrorFileChunk> chunks;
		try {
			chunks = ErrorFileChunk.listFromInputReader(br);
		} catch (IOException e) {
			return false;
		}
		
		int handled = handleErrorChunks(chunks);
		if(chunks.size() == handled){
			clearErrorEmailSentFlag();
			return true;
		}else{
			return false;
		}
	}
	
	
	/**
	 * Attempt to process the error file and retry all of the transactions inside that file
	 * This method will clear the ErrorEmailSent flag if successful
	 * @param errorFileName - Error file to process
	 * @return - true on success, false on failure
	 */
	public static boolean handleErrorFile(String errorFileName){
		File errorFile = null;
		try{
			errorFile = openErrorFile(errorFileName, true);
		}catch(Exception e){
			SMMailer.sendMessage(ERROR_EMAIL, "Access Logger Queue error file not writable",
						String.format("Unable to open/write to error file: %s", errorFileName));
			return false;
		}
		//If ERROR_FILE exists and is not empty...
		if(errorFile.exists() && errorFile.length() != 0){
			List<ErrorFileChunk> chunks = null;
			try{
				FileReader fRead = new FileReader(errorFile);
				BufferedReader in = new BufferedReader(fRead);
				chunks = ErrorFileChunk.listFromInputReader(in);
			}catch(Exception e){
				SMMailer.sendMessage(ERROR_EMAIL, "Access Logger Queue error file problem",
						String.format("There was a problem loading the error file: %s\n%s",
								errorFileName, e.getMessage()));
				logger.error("Unable to read error file:");
				e.printStackTrace();
				return false;
			}
			
			//Handle the error chunks
			int chunksHandled = handleErrorChunks(chunks);
			if(chunksHandled == chunks.size()){
				try{
					if(!errorFile.delete()){
						logger.error("Unable to delete error file after processing");
					}
				}catch(SecurityException e){
					logger.error("Unable to delete error file after processing");
					e.printStackTrace();
				}
				clearErrorEmailSentFlag();
				return true;
			}else{
				try {
					//Write the remaining chunks back to the file
					ErrorFileChunk.writeChunksToFile(chunks.subList(chunksHandled, chunks.size()),
							ERROR_FILE);
				} catch (IOException e) {
					e.printStackTrace();
					writeObjectsToLog(chunks.subList(chunksHandled, chunks.size()));
				}
				return false;
			}
		}
		//If there was no file, or it's empty... there're no errors, so return success
		clearErrorEmailSentFlag();
		return true;
	}
	
	//TODO: need to find some way to run either a file or copy/pasted error chunks from some app
	
	private static class QueueHandlerRunnable implements Runnable{
		@Override
		public void run() {
			Connection conn = null;
			try {
				System.out.format("Attempting to connect to database\n");
				conn = SMDBPool.getInstance().getDataSource().getConnection();
				conn.setAutoCommit(false);
				System.out.format("Connected.\n");
			} catch (SQLException e1) {
				logger.error("Unable to get DB Connection in Entitlements Access Logger!");
				e1.printStackTrace();
				//Exit from thread, since we can't do anything without the db access
				AccessLoggerQueue.queueHandler = null;
				return;
			}
			while(true){
				drainQueue(conn, CHUNK_SIZE);
				handleErrorFile(ERROR_FILE);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {}
			}
		}
	}
	
	protected synchronized static BlockingQueue<ProductView> getAccessQueue(){
		if(accessQueue == null){
			accessQueue = new LinkedBlockingQueue<ProductView>();
		}
		return accessQueue;
	}
	
	/**
	 * Enqueue a view to be written to the access logs
	 * Must be called after {@link startHandler}
	 * @param view
	 * @throws IllegalStateException if handler thread is not running
	 */
	public synchronized static void EnqueueAccess(ProductView view) throws IllegalStateException{
		if(queueHandler == null){
			throw new IllegalStateException("Access Logger Queue Handler is not running");
		}
		getAccessQueue().add(view);
	}
	
	/**
	 * Start up the queue processing thread for the access logger queue
	 */
	public static synchronized void startHandler(){
		if(queueHandler == null || !queueHandler.isAlive()){
			System.out.format("Starting access logger queue handler...\n");
			queueHandler = new Thread(new QueueHandlerRunnable());
			queueHandler.start();
		}
	}
	
	protected static boolean isErrorEmailSent() {
		return errorEmailSent;
	}
	protected static void setErrorEmailSent(boolean errorEmailSent) {
		AccessLoggerQueue.errorEmailSent = errorEmailSent;
	}
	public static void clearErrorEmailSentFlag(){
		setErrorEmailSent(false);
	}
}
