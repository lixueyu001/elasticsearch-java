import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;


public class RestHighLevelClientUtils {

    private static RestHighLevelClient client;

    static {
        RestClientBuilder clientBuilder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));
        /*
        //登陆认证 如果ES集群没有开启安全验证，可以跳过此设置。
        CredentialsProvider provider
                = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials
                = new UsernamePasswordCredentials(
                "username",
                "password");
        provider.setCredentials(AuthScope.ANY, credentials);
        clientBuilder
                .setHttpClientConfigCallback(builder ->
                  builder.setDefaultCredentialsProvider(provider)
                );
        */
        client = new RestHighLevelClient(clientBuilder);
    }

    public static RestHighLevelClient getClient() {
        return client;
    }
}
