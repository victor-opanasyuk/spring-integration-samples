package com.example.integration.conf;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.aws.inbound.SqsMessageDrivenChannelAdapter;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Transformers;
import org.springframework.integration.jdbc.JdbcMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.PollableChannel;

import javax.sql.DataSource;

@Configuration
public class AwsConfiguration {

    @Autowired
    private AmazonSQSAsync amazonSqs;

    @Autowired
    private DataSource dataSource;

    @Bean
    public PollableChannel inputChannel() {
        return new QueueChannel();
    }

    @Bean
    public MessageProducer sqsMessageDrivenChannelAdapter() {
        SqsMessageDrivenChannelAdapter adapter = new SqsMessageDrivenChannelAdapter(this.amazonSqs, "myQueue");
        adapter.setOutputChannel(inputChannel());
        adapter.setAutoStartup(true);
        adapter.setMessageDeletionPolicy(SqsMessageDeletionPolicy.ON_SUCCESS);
        return adapter;
    }

    @Bean
    public IntegrationFlow awsSqsToInternalFlow() {
        return IntegrationFlows.from(this::sqsMessageDrivenChannelAdapter)
                .transform(Transformers.fromJson())
                .channel(inputChannel())
                .get();
    }

    @Bean
    public IntegrationFlow awsSqsToInternalFlow1() {
        return IntegrationFlows.from(this::inputChannel)
                .aggregate(a ->
                        a.correlationStrategy(Message::getPayload)
                                .releaseStrategy(g -> g.size() > 100)
                               )
                .handle(jdbcMessageHandler())
                .get();
    }

    @Bean
    public MessageHandler jdbcMessageHandler(){
        return new JdbcMessageHandler(dataSource, "insert into");
    }
}
