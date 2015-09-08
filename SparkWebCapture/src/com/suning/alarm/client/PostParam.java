package com.suning.alarm.client;

public class PostParam {

	private static final String RECV_TYPE = "recvType";
	private static final String DELAY_MINUTES = "delay";
	private static final String GROUP_LIST = "grps";
	private static final String RECV_LIST = "recvs";
	private static final String ALARM_VALUE = "value";
	private static final String ALARM_TYPE = "type";
	private static final String HOST_IP = "ip";
	private static final String HOST_NAME = "host";
	private static final String SYSTEM_NAME = "sys";
	private String sysName;
	private String hostName;
	private String hostIP;
	private String alarmType;
	private String alarmValue;
	private String recvStr;
	private String grpStr;
	private int delayMin;
	private String recvType;

	public PostParam() {
		delayMin = -1;
	}

	public String getSysName() {
		return sysName;
	}

	public void setSysName(String sysName) {
		this.sysName = sysName;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getHostIP() {
		return hostIP;
	}

	public void setHostIP(String hostIP) {
		this.hostIP = hostIP;
	}

	public String getAlarmType() {
		return alarmType;
	}

	public void setAlarmType(String alarmType) {
		this.alarmType = alarmType;
	}

	public String getAlarmValue() {
		return alarmValue;
	}

	public void setAlarmValue(String alarmValue) {
		this.alarmValue = alarmValue;
	}

	public String getRecvStr() {
		return recvStr;
	}

	public void setRecvStr(String recvStr) {
		this.recvStr = recvStr;
	}

	public String getGrpStr() {
		return grpStr;
	}

	public void setGrpStr(String grpStr) {
		this.grpStr = grpStr;
	}

	public int getDelayMin() {
		return delayMin;
	}

	public void setDelayMin(int delayMin) {
		if (delayMin < 0)
			delayMin = 5;
		this.delayMin = delayMin;
	}

	public String getRecvType() {
		return recvType;
	}

	public void setRecvType(String recvType) {
		this.recvType = recvType;
	}

	public boolean validate() {
		if (this.getSysName() == null || "".equals(this.getSysName()))
			return false;
		if (this.getHostName() == null || "".equals(this.getHostName()))
			return false;
		if (this.getHostIP() == null || "".equals(this.getHostIP()))
			return false;
		if (this.getAlarmType() == null || "".equals(this.getAlarmType()))
			return false;
		if (this.getAlarmValue() == null || "".equals(this.getAlarmValue()))
			return false;
		if ((this.getRecvStr() == null || "".equals(this.getRecvStr()))
				&& (this.getGrpStr() == null || "".equals(this.getGrpStr())))
			return false;

		if (this.getDelayMin() >= 0 && this.getDelayMin() != 0
				&& this.getDelayMin() != 1 && this.getDelayMin() != 5
				&& this.getDelayMin() != 10 && this.getDelayMin() != 30
				&& this.getDelayMin() != 60)
			return false;
		if (this.getRecvType() != null) {
			if (!this.getRecvType().equals("ALL")
					&& !this.getRecvType().equals("SMS")
					&& !this.getRecvType().equals("EMAIL"))
				return false;
		}
		return true;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(format(SYSTEM_NAME, this.getSysName(), false));
		sb.append(format(HOST_NAME, this.getHostName()));
		sb.append(format(HOST_IP, this.getHostIP()));
		sb.append(format(ALARM_TYPE, this.getAlarmType()));
		sb.append(format(ALARM_VALUE, this.getAlarmValue()));
		sb.append(format(RECV_LIST, this.getRecvStr()));
		sb.append(format(GROUP_LIST, this.getGrpStr()));
		if (this.getDelayMin() >= 0)
			sb.append(format(DELAY_MINUTES, String.valueOf(this.getDelayMin())));
		sb.append(format(RECV_TYPE, this.getRecvType()));

		return sb.toString();
	}

	private String format(String param, String value) {
		return format(param, value, true);
	}

	private String format(String param, String value, boolean preSep) {
		String sep = null;
		String res = null;
		if (preSep)
			sep = "&";
		else
			sep = "";
		if (param == null || param.equals("") || value == null)
			res = "";
		else
			res = sep + param + "=" + value;
		return res;
	}
}
