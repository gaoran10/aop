package io.streamnative.pulsar.handlers.amqp.admin.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ChannelBean {

    @JsonProperty("connection_details")
    private ConnectionDetailsBean connectionDetails;
    @JsonProperty("consumer_count")
    private int consumerCount;
    @JsonProperty("messages_unacknowledged")
    private long messagesUnacknowledged;
    @JsonProperty("messages_uncommitted")
    private long messages_uncommitted;
    @JsonProperty("messages_unconfirmed")
    private long messages_unconfirmed;
    private String name;
    private String node;
    private int number;
    private String user;
    @JsonProperty("user_who_performed_action")
    private String userWhoPerformedAction;
    private String vhost;

}
