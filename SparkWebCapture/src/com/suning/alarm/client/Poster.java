package com.suning.alarm.client;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class Poster {
	public static final String URL = "http://192.168.85.43:8080/com.suning.alarm-0.1/alarm";
	
	public String postService(String url, String data, String encode) {
		CloseableHttpClient client = HttpClients.createDefault();
		HttpPost post = new HttpPost(url);
		StringEntity myEntity = new StringEntity(data, ContentType.create(
				"application/x-www-form-urlencoded", encode));
		post.setEntity(myEntity);
		String responseContent = null;
		CloseableHttpResponse response = null;
		try {
			response = client.execute(post);
			if (response.getStatusLine().getStatusCode() == 200) {
				HttpEntity entity = response.getEntity();
				responseContent = EntityUtils.toString(entity, encode);
			}
		} catch (ClientProtocolException e) {
			responseContent = "e:" + e.getClass().getName();
		} catch (IOException e) {
			responseContent = "e:" + e.getClass().getName();
		} finally {
			try {
				if (response != null)
					response.close();

			} catch (IOException e) {
				responseContent = "e:" + e.getClass().getName();
			} finally {
				try {
					if (client != null)
						client.close();
				} catch (IOException e) {
					responseContent = "e:" + e.getClass().getName();
				}
			}
		}
		return responseContent;
	}

	public String postService(String url, String data) {
		return postService(url, data, "UTF-8");
	}

	private Poster() {
	}

	private static Poster poster;

	private synchronized static void init() {
		if (poster == null) {
			poster = new Poster();
		}
	}

	public static Poster getPoster() {
		if (poster == null) {
			init();
		}
		return poster;
	}
}
