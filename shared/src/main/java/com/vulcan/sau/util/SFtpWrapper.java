package com.vulcan.sau.util;

import java.util.Properties;
import java.util.Vector;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

/** This is a simple wrapper around the commons-vfs class
 * 
 * @author Linh Tran
 * @version 1.0.1
 * 	Cleaned up import list and add <String> to all instance of use of Vector to avoid compiler warning.
 */

public class SFtpWrapper {
	private Properties config = null;
	private JSch jsch = null;
	Session session = null;    
	private ChannelSftp sftpChannel = null; 
	
	public SFtpWrapper() {
		jsch = new JSch();
		
		config = new java.util.Properties();
		config.put("StrictHostKeyChecking", "no");
	}

	/** A convenience method for connecting and logging in */
	public boolean connectAndLogin(String host, String userName, String password) throws JSchException {
		Session session = jsch.getSession(userName, host, 22);    
		session.setConfig(config);
		session.setPassword(password.getBytes());
		session.connect();

		Channel channel = session.openChannel("sftp");
		channel.connect();

		sftpChannel = (ChannelSftp) channel;
		
		return true;
	}

	/** Download a file from the server, and save it to the specified local file */
	public boolean downloadFile (String remoteFile, String localFile) throws SftpException {
		sftpChannel.get(remoteFile, localFile);
        return true;
	}

	/** Get the list of files in the current remoteDataDirectory as a Vector of Strings 
	 * (excludes subdirectories) */
	public Vector<String> listFileNames (String path) throws SftpException {
		Vector<String> v = new Vector<String>();
		Vector<LsEntry> files = (Vector<LsEntry>) sftpChannel.ls(path);
		
		for (LsEntry fe: files) {
			v.addElement(fe.getFilename());
		}
		
		return v;
	}

	/** Get the list of files in the current directory as a Vector of Strings 
	 * This version allows for file filter to be provided 
	 * (excludes subdirectories) */
	public Vector<String> listFileNames (String path, FileFilter filter) throws SftpException {
		Vector<String> v = new Vector<String>();

		Vector<LsEntry> files = (Vector<LsEntry>) sftpChannel.ls(path);
		
		for (LsEntry fe: files) {
			String fileName = fe.getFilename();
			if (filter.accept(fileName)) {
				v.addElement(fileName);
			}
		}
		
		return v;
	}
	
	public void release() {
		if (sftpChannel != null) sftpChannel.disconnect();
		if (session != null) session.disconnect();
	}
	
	public interface FileFilter {
		boolean accept(String fileName);
	}
}
