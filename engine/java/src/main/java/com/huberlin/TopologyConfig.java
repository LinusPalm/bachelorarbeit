package com.huberlin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopologyConfig {
    private String ip;

    private int port;

    private HashMap<String, String> forwardingTable;

    private Collection<String> connection;

    public int getPort() { return port; }

    @JsonProperty("port")
    public void setPort(int port) { this.port = port; }

    public HashMap<String, String> getForwardingTable() {
        return forwardingTable;
    }

    @JsonProperty("forwarding_table")
    public void setForwardingTable(HashMap<String, String> forwardingTable) {
        this.forwardingTable = forwardingTable;
    }

    public Collection<String> getConnection() {
        return connection;
    }

    @JsonProperty("connection")
    public void setConnection(Collection<String> connection) {
        this.connection = connection;
    }

    public String getIp() { return ip; }

    @JsonProperty("ip")
    public void setIp(String ip) { this.ip = ip; }
}
