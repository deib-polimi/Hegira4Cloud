package eu.modaclouds.db.api;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Status {
	private String status;
	private String message;
	private String error_code;
	public Status(String status, String message, String error_code) {
		this.status = status;
		this.message = message;
		this.error_code = error_code;
	}
	public Status(String status) {
		this.status = status;
	}
	public Status(String status, String message) {
		this.status = status;
		this.message = message;
	}
	public Status() {
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getError_code() {
		return error_code;
	}
	public void setError_code(String error_code) {
		this.error_code = error_code;
	}
	
	
}
