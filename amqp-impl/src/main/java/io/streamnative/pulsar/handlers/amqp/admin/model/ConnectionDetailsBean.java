package io.streamnative.pulsar.handlers.amqp.admin.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class ConnectionDetailsBean {

    private String name;
    @JsonProperty("peer_host")
    private String peerHost;
    @JsonProperty("peer_port")
    private String peerPort;

}
