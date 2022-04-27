package org.apache.hadoop.hive.ql.omnidata.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Ndp hive configuration
 *
 * @since 2021-11-09
 */
public class NdpConf {
    public final String NDP_ENABLED = "hive.sql.ndp.enabled";

    public final String NDP_FILTER_SELECTIVITY_ENABLE = "hive.sql.ndp.filter.selectivity.enable";

    public final String NDP_TABLE_SIZE_THRESHOLD = "hive.sql.ndp.table.size.threshold";

    public final String NDP_FILTER_SELECTIVITY = "hive.sql.ndp.filter.selectivity";

    public final String NDP_UDF_WHITELIST = "hive.sql.ndp.udf.whitelist";

    public final String NDP_SDI_PORT = "hive.sql.ndp.sdi.port";

    public final String NDP_GRPC_SSL_ENABLED = "hive.sql.ndp.grpc.ssl.enabled";

    public final String NDP_GRPC_CLIENT_CERT_FILE_PATH = "hive.sql.ndp.grpc.client.cert.file.path";

    public final String NDP_GRPC_CLIENT_PRIVATE_KEY_FILE_PATH = "hive.sql.ndp.grpc.client.private.key.file.path";

    public final String NDP_GRPC_TRUST_CA_FILE_PATH = "hive.sql.ndp.grpc.trust.ca.file.path";

    public final String NDP_PKI_DIR = "hive.sql.ndp.pki.dir";

    public final String NDP_ZOOKEEPER_QUORUM_SERVER = "hive.sql.ndp.zookeeper.quorum.server";

    public final String NDP_ZOOKEEPER_STATUS_NODE = "hive.sql.ndp.zookeeper.status.node";

    public final String NDP_ZOOKEEPER_CONF_PATH = "hive.sql.ndp.zookeeper.conf.path";

    public final String NDP_ZOOKEEPER_SECURITY_ENABLED = "hive.sql.ndp.zookeeper.security.enabled";

    public final String NDP_ZOOKEEPER_CONNECTION_TIMEOUT = "hive.sql.ndp.zookeeper.connection.timeoutMs";

    public final String NDP_ZOOKEEPER_SESSION_TIMEOUT = "hive.sql.ndp.zookeeper.session.timeoutMs";

    public final String NDP_ZOOKEEPER_RETRY_INTERVAL = "hive.sql.ndp.zookeeper.retry.intervalMs";

    public final String NDP_REPLICATION_NUM = "hive.sql.ndp.replication.num";

    private final Properties hiveProperties;

    public NdpConf(Configuration conf) {
        this.hiveProperties = HiveConf.getProperties(conf);
    }

    public Boolean getNdpEnabled() {
        return Boolean.valueOf(hiveProperties.getProperty(NDP_ENABLED, "true").toLowerCase());
    }

    public Boolean getNdpFilterSelectivityEnable() {
        return Boolean.valueOf(hiveProperties.getProperty(NDP_FILTER_SELECTIVITY_ENABLE, "false").toLowerCase());
    }

    public int getNdpTablesSizeThreshold() {
        return Integer.parseInt(hiveProperties.getProperty(NDP_TABLE_SIZE_THRESHOLD, "10240").toLowerCase());
    }

    public Double getNdpFilterSelectivity() {
        double selectivity = Double.parseDouble(
                hiveProperties.getProperty(NDP_FILTER_SELECTIVITY, "0.5").toLowerCase());
        checkArgument(selectivity >= 0 && selectivity <= 1.0,
                String.format("The %s value must be in [0.0, 1.0].", NDP_FILTER_SELECTIVITY));
        return selectivity;
    }

    public String[] getNdpUdfWhitelist() {
        String[] whiteList = hiveProperties.getProperty(NDP_UDF_WHITELIST, "").split(",");
        checkArgument(whiteList.length == 0, String.format("The %s is empty", NDP_UDF_WHITELIST));
        return whiteList;
    }

    public String getNdpSdiPort() {
        int port = Integer.parseInt(hiveProperties.getProperty(NDP_SDI_PORT, "9105").toLowerCase());
        checkArgument(port > 0, String.format("The %s value must be positive", NDP_SDI_PORT));
        return Integer.toString(port);
    }

    public Boolean getNdpGrpcSslEnabled() {
        return Boolean.valueOf(hiveProperties.getProperty(NDP_GRPC_SSL_ENABLED, "false").toLowerCase());
    }

    public String getNdpGrpcClientCertFilePath() {
        return hiveProperties.getProperty(NDP_GRPC_CLIENT_CERT_FILE_PATH, "/opt/conf/client.crt");
    }

    public String getNdpGrpcClientPrivateKeyFilePath() {
        return hiveProperties.getProperty(NDP_GRPC_CLIENT_PRIVATE_KEY_FILE_PATH, "/opt/conf/client.pem");
    }

    public String getNdpGrpcTrustCaFilePath() {
        return hiveProperties.getProperty(NDP_GRPC_TRUST_CA_FILE_PATH, "/opt/conf/ca.crt");
    }

    public String getNdpPkiDir() {
        return hiveProperties.getProperty(NDP_PKI_DIR, "/opt/conf/");
    }

    public String getNdpZookeeperQuorumServer() {
        return hiveProperties.getProperty(NDP_ZOOKEEPER_QUORUM_SERVER, "agent1:2181");
    }

    public int getNdpZookeeperConnectionTimeout() {
        int timeout = Integer.parseInt(
                hiveProperties.getProperty(NDP_ZOOKEEPER_CONNECTION_TIMEOUT, "15000").toLowerCase());
        checkArgument(timeout > 0, String.format("The %s value must be positive", NDP_ZOOKEEPER_CONNECTION_TIMEOUT));
        return timeout;
    }

    public int getNdpZookeeperSessionTimeout() {
        int timeout = Integer.parseInt(
                hiveProperties.getProperty(NDP_ZOOKEEPER_SESSION_TIMEOUT, "60000").toLowerCase());
        checkArgument(timeout > 0, String.format("The %s value must be positive", NDP_ZOOKEEPER_SESSION_TIMEOUT));
        return timeout;
    }

    public int getNdpZookeeperRetryInterval() {
        int retryInterval = Integer.parseInt(
                hiveProperties.getProperty(NDP_ZOOKEEPER_RETRY_INTERVAL, "1000").toLowerCase());
        checkArgument(retryInterval > 0, String.format("The %s value must be positive", NDP_ZOOKEEPER_RETRY_INTERVAL));
        return retryInterval;
    }

    public String getNdpZookeeperConfPath() {
        return hiveProperties.getProperty(NDP_ZOOKEEPER_CONF_PATH, "/opt/hadoopclient/ZooKeeper/zookeeper/conf/");
    }

    public Boolean getNdpZookeeperSecurityEnabled() {
        return Boolean.valueOf(hiveProperties.getProperty(NDP_ZOOKEEPER_SECURITY_ENABLED, "true").toLowerCase());
    }

    public String getNdpZookeeperStatusNode() {
        return hiveProperties.getProperty(NDP_ZOOKEEPER_STATUS_NODE, "/sdi/status");
    }

    public int getNdpReplicationNum() {
        int replicationNum = Integer.parseInt(hiveProperties.getProperty(NDP_REPLICATION_NUM, "3").toLowerCase());
        checkArgument(replicationNum > 0, String.format("The %s value must be positive", NDP_REPLICATION_NUM));
        return replicationNum;
    }
}