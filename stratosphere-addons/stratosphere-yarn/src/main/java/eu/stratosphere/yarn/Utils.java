/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package eu.stratosphere.yarn;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;

public class Utils {
	
	private static final Log LOG = LogFactory.getLog(Utils.class);
	

	public static void getStratosphereConfiguration(String confDir) {
		GlobalConfiguration.loadConfiguration(confDir);
	}
	
	private static void addPathToConfig(Configuration conf, File path) {
		// chain-in a new classloader
		URL fileUrl = null;
		try {
			 fileUrl = path.toURL();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		URL[] urls = {fileUrl};
		ClassLoader cl = new URLClassLoader(urls, conf.getClassLoader());
		conf.setClassLoader(cl);
	}
	private static void setDefaultConfValues(Configuration conf) {
		if(conf.get("fs.hdfs.impl",null) == null) {
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		}
		if(conf.get("fs.file.impl",null) == null) {
			conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		}
	}
	public static Configuration initializeYarnConfiguration() {
		Configuration conf = new YarnConfiguration();
		String envs[] = { "YARN_CONF_DIR", "HADOOP_CONF_DIR", "HADOOP_CONF_PATH" };
		for(int i = 0; i < envs.length; ++i) {
			String confPath = System.getenv(envs[i]);
			if (confPath != null) {
				LOG.info("Found "+envs[i]+", adding it to configuration");
				addPathToConfig(conf, new File(confPath));
				setDefaultConfValues(conf);
				return conf;
			}
		}
		LOG.info("Could not find HADOOP_CONF_PATH, using HADOOP_HOME.");
		String hadoopHome = null;
		try {
			hadoopHome = Shell.getHadoopHome();
		} catch (IOException e) {
			LOG.fatal("Unable to get hadoop home. Please set HADOOP_HOME variable!", e);
			System.exit(1);
		}
		File tryConf = new File(hadoopHome+"/etc/hadoop");
		if(tryConf.exists()) {
			LOG.info("Found configuration using hadoop home.");
			addPathToConfig(conf, tryConf);
		} else {
			tryConf = new File(hadoopHome+"/conf");
			if(tryConf.exists()) {
				addPathToConfig(conf, tryConf);
			}
		}
		setDefaultConfValues(conf);
		return conf;
	}
	
	public static void setupEnv(Configuration conf, Map<String, String> appMasterEnv) {
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
		}
		Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*");
	}
	
	 
	/**
	 * 
	 * @return Path to remote file (usually hdfs)
	 * @throws IOException
	 */
	public static Path setupLocalResource(Configuration conf, FileSystem fs, int appId, Path localRsrcPath, LocalResource appMasterJar)
			throws IOException {
		// copy to HDFS
		String suffix = ".stratosphere/" + appId + "/" + localRsrcPath.getName();
		
	    Path dst = new Path(fs.getHomeDirectory(), suffix);
	    
	    fs.copyFromLocalFile(localRsrcPath, dst);
	    registerLocalResource(fs, dst, appMasterJar);
	    return dst;
	}
	
	public static void registerLocalResource(FileSystem fs, Path remoteRsrcPath, LocalResource appMasterJar) throws IOException {
		FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
		appMasterJar.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
		appMasterJar.setSize(jarStat.getLen());
		appMasterJar.setTimestamp(jarStat.getModificationTime());
		appMasterJar.setType(LocalResourceType.FILE);
		appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
	}
	
	
}
