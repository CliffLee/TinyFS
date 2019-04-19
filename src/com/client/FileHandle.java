package com.client;

import java.io.Serializable;

public class FileHandle implements Serializable {
	private static final long serialVersionUID = 1L;

	public String temp;
	public String filepath;
	
	public FileHandle() {
		temp = "something";
	}
	
	public void setToHandle(FileHandle fh) {
		this.temp = fh.temp;
		this.filepath = fh.filepath;
	}

	public void setFilePath(String path) {
		this.filepath = path;
	}
}
