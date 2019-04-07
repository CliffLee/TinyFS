package com.client;

public class RID {
	public String macAddress;
	public int key;

	public String resolve() {
		return macAddress + String.valueOf(this.key);
	}
}
