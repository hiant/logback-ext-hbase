/*
 * Copyright 2016 heihuwudi@gmail.com
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

package io.github.hiant;

import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

import ch.qos.logback.core.spi.ContextAwareBase;

/**
 * @author heihuwudi@gmail.com Created By: 2016.1.25.14:27
 */
public class SimpleConfigurationFactory extends ContextAwareBase implements ConfigurationFactory {

    private boolean started;
    private String resource = null;
    private String quorum = null;
    private String port = null;
    private Configuration configuration;

    public boolean isStarted() {
        return started;
    }

    public void start() {
        Configuration configuration = getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();
            ClusterStatus status = admin.getClusterStatus();
            addInfo("HBase version = " + status.getHBaseVersion());
            addInfo("HBase clusterId = " + status.getClusterId());
            addInfo("HBase servers = " + status.getServers());
            started = true;
        } catch (IOException e) {
            addWarn("Could not discover the connection to use.", e);
        }
    }

    public void stop() {
        started = false;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    @Override
    public synchronized Configuration getConfiguration() {
        if (configuration == null) {
            configuration = new Configuration();
            if (!Strings.isNullOrEmpty(resource)) {
                configuration.addResource(resource);
            }
            if (!Strings.isNullOrEmpty(quorum)) {
                configuration.set("hbase.zookeeper.quorum", quorum);
            }
            if (!Strings.isNullOrEmpty(port)) {
                configuration.set("hbase.zookeeper.property.clientPort", port);
            }
        }
        return configuration;
    }

}
