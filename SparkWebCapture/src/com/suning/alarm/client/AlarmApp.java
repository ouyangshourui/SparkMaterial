package com.suning.alarm.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

public class AlarmApp {

	public static String randomHost() {
		Random r = new Random();
		return "主机" + String.valueOf(r.nextInt(100));
	}

	public static String randomValue() {
		Random r = new Random();
		return String.valueOf(r.nextFloat());
	}

	public static String randomIP() {
		try {
			Random r = new Random();
			InetAddress addr = InetAddress.getByAddress(new byte[] {
					(byte) r.nextInt(), (byte) r.nextInt(), (byte) r.nextInt(),
					(byte) r.nextInt() });
			return addr.getHostAddress();
		} catch (UnknownHostException e) {
			return "127.0.0.1";
		}
	}

	public static String alarm(String sysName, String hostName, String hostIP,
			String alarmType, String alarmValue, String grpStr) {
		String recvType = "ALL";
		int delayMin = 5;
		String resp = alarm(sysName, hostName, hostIP, alarmType, alarmValue,
				null, grpStr, delayMin, recvType);
		return resp;
	}

	public static String alarm(String sysName, String hostName, String hostIP,
			String alarmType, String alarmValue, String recvStr, String grpStr) {
		String recvType = "ALL";
		int delayMin = 5;
		String resp = alarm(sysName, hostName, hostIP, alarmType, alarmValue,
				recvStr, grpStr, delayMin, recvType);
		return resp;
	}

	public static String alarm(String sysName, String hostName, String hostIP,
			String alarmType, String alarmValue, String recvStr, String grpStr,
			int delayMin, String recvType) {
		PostParam pp = new PostParam();
		pp.setSysName(sysName);
		pp.setHostName(hostName);
		pp.setHostIP(hostIP);
		pp.setAlarmType(alarmType);
		pp.setAlarmValue(alarmValue);
		pp.setRecvStr(recvStr);
		pp.setGrpStr(grpStr);
		pp.setDelayMin(delayMin);
		pp.setRecvType(recvType);
		System.out.println(pp);

		Poster poster = Poster.getPoster();
		String resp = "UNKNOWN";
		if (pp.validate()) {
			String url = Poster.URL;
			resp = poster.postService(url, pp.toString());
		}
		return resp;
	}

	public static void alarm(String sysName, int delayMin, int count) throws UnknownHostException {
		String alarmType = "例外";
		String recvStr = "欧锐";
		String grpStr = "平台开发部";
		String recvType = "ALL";
		InetAddress ia = InetAddress.getLocalHost();
		for (int i = 0; i < count; i++) {
			System.out.println("============== " + i + " =============");
			String resp = alarm(sysName, ia.getHostName(),ia.getHostAddress(),
					alarmType, randomValue(), recvStr, grpStr, delayMin,
					recvType);
	
			System.out.println(resp);
			System.out.println("================================");
			System.out.println();
		}
	}
	public static void alarm(String sysName, int delayMin,String alarmType) throws UnknownHostException {
		String recvStr = "欧锐";
		String grpStr = "平台开发部";
		String recvType = "ALL";
		InetAddress ia = InetAddress.getLocalHost();
		    System.out.println("================================");
			String resp = alarm(sysName, ia.getHostName(),ia.getHostAddress(),
					alarmType, alarmType, recvStr, grpStr, delayMin,
					recvType);
	
			System.out.println(resp);
			System.out.println("================================");
			System.out.println();		
	}

}
