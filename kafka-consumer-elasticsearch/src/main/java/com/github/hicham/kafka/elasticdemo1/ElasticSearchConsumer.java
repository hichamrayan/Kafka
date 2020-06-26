package com.github.hicham.kafka.elasticdemo1;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumer{
    // don't do that if you run a local ES
        public static RestHighLevelClient createClient(){
            //https://wbv8ctkfgy:lsu39bn9vw@kafka-project-2025477351.us-west-2.bonsaisearch.net:443
            String hostname="kafka-project-2025477351.us-west-2.bonsaisearch.net";
            String username="wbv8ctkfgy";
            String password="lsu39bn9vw";
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443,"https"))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(
                                HttpAsyncClientBuilder httpClientBuilder) {
                            return httpClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
            RestHighLevelClient client=new RestHighLevelClient(builder);
            return client;
    }

    public static void main(String[] args) throws IOException {
            Logger logger= LoggerFactory.getLogger("Class");
        RestHighLevelClient client=createClient();
        String jsonString="{\"name\":\"Rayan\"}";
        IndexRequest indexRequest=new IndexRequest("twitter","tweets").source(jsonString, XContentType.JSON);
        IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
        logger.info(indexResponse.getId());

    }
}
