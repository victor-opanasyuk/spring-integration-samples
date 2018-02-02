package com.example.integration.conf;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.aws.inbound.SqsMessageDrivenChannelAdapter;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Transformers;
import org.springframework.messaging.PollableChannel;

@Configuration
public class AwsConfiguration {

    @Autowired
    private AmazonSQSAsync amazonSqs;

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
        return IntegrationFlows.from(this::awsSqsToInternalFlow)
                .channel(inputChannel())
                .get();
    }



}
