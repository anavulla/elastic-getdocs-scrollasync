package com.mycom.elasticsearch.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 
 * @author anavulla
 *
 */
@Configuration
public class ElasticSearchConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConfig.class);

	@Value("${elasticsearch.host}")
	private String elasticsearch_host;

	@Value("${elasticsearch.port}")
	private int elasticsearch_port;

	@Value("${elasticsearch.username}")
	private String elasticsearch_username;

	@Value("${elasticsearch.password}")
	private String elasticsearch_password;

	@Value("${trustore.location}")
	private String truststore_location;

	@Value("$trustore.password}")
	private String truststore_password;

	private static final String PROTOCOL = "https";

	RestHighLevelClient client = null;

	@Bean
	public RestHighLevelClient client() {

		try {
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,
					new UsernamePasswordCredentials(elasticsearch_username, elasticsearch_password));

			RestClientBuilder builder = RestClient
					.builder(new HttpHost(elasticsearch_host, elasticsearch_port, PROTOCOL))
					.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

						@Override
						public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

							return httpClientBuilder.setSSLContext(setSSLcontext())
									.setDefaultCredentialsProvider(credentialsProvider);
						}

					});
			this.client = new RestHighLevelClient(builder);

		} catch (Exception e) {
			LOGGER.error("Exception initializing elastic search connection:" + e.getMessage());
		}
		return this.client;
	}

	private SSLContext setSSLcontext() {
		SSLContext sslcontext = null;
		try {
			File trustStoreLocation = new File(truststore_location);
			InputStream trustStore = new FileInputStream(trustStoreLocation);
			KeyStore keystore = KeyStore.getInstance("jks");
			char[] password = truststore_password.trim().toCharArray();
			keystore.load(trustStore, password);
			TrustManagerFactory trustManagerFactory = TrustManagerFactory
					.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(keystore);
			TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
			sslcontext = SSLContext.getInstance("TLS");
			sslcontext.init(null, trustManagers, null);

		} catch (Exception e) {
			LOGGER.error("Exception initializing elastic search ssl context:" + e.getMessage());
		}
		return null;
	}
}
