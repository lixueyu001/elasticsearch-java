import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchJava {
    private static RestHighLevelClient client = RestHighLevelClientUtils.getClient();

    public static void main(String[] args) throws IOException {
        index();
//        update();
//        updateByQuery();
//        get();
//        delete();
//        bulk();
//        search();
//        delete();
//        deleteByQuery();
        client.close();
    }

    /**
     * 向一个名为 posts 的索引中写入数据。
     *
     * @throws IOException
     */
    public static void index() throws IOException {
        //构建 IndexRequest 并设置索引名称
        IndexRequest request = new IndexRequest("posts");
        //文档 ID （可选，不指定 ES 会主动生成）
        request.id("1");
        //设置文档内容 此处选择以 JSON 字符串的形式构建文档内容，
        //其还支持 XContentBuilder、Map、Object key-pairs 的形式
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2021-11-09\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        //Index API 请求发送至ES
        IndexResponse indexResponse =
                client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse);
    }
//输出：IndexResponse[index=posts,type=_doc,id=1,version=1,result=updated,seqNo=2,primaryTerm=1,shards={"total":2,"successful":1,"failed":0}]

    /**
     * 将索引 posts 中 id 为1的文档 message 值更新为 "trying out update"
     *
     * @throws IOException
     */
    public static void update() throws IOException {
        //构建 UpdateRequest 设置索引名称和文档 ID
        UpdateRequest request = new UpdateRequest("posts", "1");
        //设置更新内容 构建形式可类比 Index API
        String jsonString = "{" +
                "\"message\":\"trying out Update\"" +
                "}";
        request.doc(jsonString, XContentType.JSON);
        //upsert 方法可以实现文档存在则更新，不存在则新增的效果。
        //request.upsert(jsonString, XContentType.JSON);
        UpdateResponse updateResponse =
                client.update(request, RequestOptions.DEFAULT);
        System.out.println(updateResponse);
    }
//输出：UpdateResponse[index=posts,type=_doc,id=1,version=2,seqNo=3,primaryTerm=1,result=updated,shards=ShardInfo{total=2, successful=1, failures=[]}]

    /**
     * 将索引 posts 中所有 user=kimchy 的文档 message 字段更新为 "trying out UpdateByQuery"
     *
     * @throws IOException
     */
    public static void updateByQuery() throws IOException {
        //构建 UpdateByQueryRequest 并设置索引名称
        UpdateByQueryRequest request = new UpdateByQueryRequest("posts");
        //版本冲突时继续执行
        request.setConflicts("proceed");
        //查询条件
        request.setQuery(new TermQueryBuilder("user", "kimchy"));
        //设置更新脚本
        Map<String, Object> params = new HashMap<>();
        params.put("message", "trying out UpdateByQuery");
        Script script = new Script(ScriptType.INLINE,
                "painless",
                "ctx._source.message = params.message", params);
        request.setScript(script);
        BulkByScrollResponse bulkResponse =
                client.updateByQuery(request, RequestOptions.DEFAULT);
        System.out.println(bulkResponse);
    }
//输出：BulkByScrollResponse[took=394ms,timed_out=false,sliceId=null,updated=1,created=0,deleted=0,batches=1,versionConflicts=0,noops=0,retries=0,throttledUntil=0s,bulk_failures=[],search_failures=[]]

    /**
     * 对 post 索引批量执行写入、更新、删除的操作
     *
     * @throws IOException
     */
    public static void bulk() throws IOException {
        //构建 BulkRequest 
        // 可添加多个DeleteRequest、UpdateRequest、IndexRequest
        BulkRequest request = new BulkRequest();
        String jsonString = "{" +
                "\"user\":\"radish\"," +
                "\"postDate\":\"2021-11-09\"," +
                "\"message\":\"trying out bulk\"" +
                "}";
        request.add(new IndexRequest("posts").id("2")
                .source(jsonString, XContentType.JSON));
//        request.add(new DeleteRequest("posts", "1"));
        request.add(new UpdateRequest("posts", "1")
                .doc(XContentType.JSON, "message", "trying out bulk"));

        BulkResponse bulkResponse =
                client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("bulk hasFailures:" + bulkResponse.hasFailures());
    }
//输出：bulk hasFailures:false

    /**
     * 获取 posts 索引中 id 为1的文档
     *
     * @throws IOException
     */
    public static void get() throws IOException {
        //构建 GetRequest 并设置索引名称和文档 id
        GetRequest getRequest = new GetRequest("posts", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println(sourceAsMap);
    }
//输出：{postDate=2021-11-09, message=trying out Elasticsearch, user=kimchy}

    /**
     * 对 posts 进行所有文档搜索
     *
     * @throws IOException
     */
    public static void search() throws IOException {
        //构建 SearchRequest 并设置索引名称
        SearchRequest searchRequest = new SearchRequest("posts");
        //设置搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //要获取返回的文档，首先要获取 SearchHits
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }
//输出：{postDate=2021-11-09, message=trying out bulk, user=kimchy}
//{postDate=2021-11-09, message=trying out bulk, user=radish}

    /**
     * 删除 posts 索引 id 为1的文档
     *
     * @throws IOException
     */
    public static void delete() throws IOException {
        //构建 DeleteRequest 并设置索引名称和文档 ID
        DeleteRequest request = new DeleteRequest("posts", "1");
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(deleteResponse);
    }
//输出：DeleteResponse[index=posts,type=_doc,id=1,version=15,result=deleted,shards=ShardInfo{total=2, successful=1, failures=[]}]

    /**
     * 删除索引 posts 中所有 user=kimchy 的文档
     *
     * @throws IOException
     */
    public static void deleteByQuery() throws IOException {
        //构建 DeleteByQueryRequest 并指定索引名称
        DeleteByQueryRequest request = new DeleteByQueryRequest("posts");
        request.setQuery(new TermQueryBuilder("user", "radish"));
        BulkByScrollResponse bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
        System.out.println(bulkResponse);
    }
//输出：BulkByScrollResponse[took=13ms,timed_out=false,sliceId=null,updated=0,created=0,deleted=0,batches=0,versionConflicts=0,noops=0,retries=0,throttledUntil=0s,bulk_failures=[],search_failures=[]]

}
