/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.server.es;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedElasticSearch {

	private static final String ES_PATH_DATA = "path.data";
	
	private static final String ES_PATH_HOME = "path.home";

	private static final Logger logger = LoggerFactory.getLogger(EmbeddedElasticSearch.class);

	public static final int DEFAULT_PORT = 9200;
	public static final String DEFAULT_CLUSTER_NAME = "elasticsearch_test";
	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final String DEFAULT_SETTING_FILE = "embedded-es.yml";

	private static Node instance; 
	private static Client client;
	private static File dataDir;

	private static class PluginConfigurableNode extends Node {
	    public PluginConfigurableNode(Settings preparedSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
	        super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), classpathPlugins);
	    }
	}
	
	public static void start() throws Exception {
		start(DEFAULT_CLUSTER_NAME, DEFAULT_HOST, DEFAULT_PORT, true);
	}

	public static synchronized void start(String clusterName, String host, int port, boolean enableTransportClient) throws Exception{

		if (instance != null && !instance.isClosed()) {
			logger.info("Elastic Search is already running on port {}", getPort());
			return;
		}

		final Settings settings = getSettings(clusterName, host, port, enableTransportClient);
		setupDataDir(settings);

		logger.info("Starting ElasticSearch for cluster {} ", settings.get("cluster.name"));
		instance = new PluginConfigurableNode(settings, Arrays.asList(Netty4Plugin.class));
		instance.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					instance.close();
				} catch (IOException e) {
					logger.error("Error closing ElasticSearch");
				}
			}
		});
		logger.info("ElasticSearch cluster {} started in local mode on port {}", instance.settings().get("cluster.name"), getPort());
		client = instance.client();
	}

	private static void setupDataDir(Settings settings) {
		String path = settings.get(ES_PATH_DATA);
		cleanDataDir(path);
		createDataDir(path);
	}

	public static void cleanDataDir(String path) {
		try {
			logger.info("Deleting contents of data dir {}", path);
			File f = new File(path);
			if (f.exists()) {
				FileUtils.cleanDirectory(new File(path));
			}
		} catch (IOException e) {
			logger.error("Failed to delete ES data dir");
		}
	}

	private static Settings getSettings(String clusterName, String host, int port, boolean enableTransportClient) throws IOException {
		dataDir = Files.createTempDirectory(clusterName+"_"+System.currentTimeMillis()+"data").toFile();
		File homeDir = Files.createTempDirectory(clusterName+"_"+System.currentTimeMillis()+"-home").toFile();
		Settings.Builder settingsBuilder = Settings.builder()
				.put("cluster.name", clusterName)
				.put("http.host", host)
				.put("http.port", port)
				.put(ES_PATH_DATA, dataDir.getAbsolutePath())
				.put(ES_PATH_HOME, homeDir.getAbsolutePath())
				.put("http.enabled", true)
				.put("script.inline", true)
				.put("script.stored", true)
				.put("node.data", true)
				.put("http.enabled", true)
				.put("http.type", "netty4")
				.put("transport.type", "netty4");
				
		return settingsBuilder.build();
	}

	private static void createDataDir(String dataDirLoc) {
		try {
			Path dataDirPath = FileSystems.getDefault().getPath(dataDirLoc);
			Files.createDirectories(dataDirPath);
			dataDir = dataDirPath.toFile();
		} catch (IOException e) {
			logger.error("Failed to create data dir");
		}
	}

	public static Client getClient() {
		if (instance == null || instance.isClosed()) {
			logger.error("Embedded ElasticSearch is not Initialized and started, please call start() method first");
			return null;
		}
		return client;
	}

	private static String getPort() {
		return instance.settings().get("http.port");
	}

	public static synchronized void stop() throws Exception {

		if (instance != null && !instance.isClosed()) {
			String port = getPort();
			logger.info("Stopping Elastic Search");
			instance.close();
			logger.info("Elastic Search on port {} stopped", port);
		}

	}
}
