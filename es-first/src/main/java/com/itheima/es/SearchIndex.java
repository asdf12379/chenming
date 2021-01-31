package com.itheima.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

/**
 * @author chenming
 * @date 2020/10/25 -  18:31
 */
public class SearchIndex {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        //创建一个settings对象
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();
        //创建一个TransPortClient对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9303));

    }

    private void search(QueryBuilder queryBuilder) throws Exception {
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                //设置分页信息  从0条开始  每页显示5行数据
                .setFrom(0)
                .setSize(5)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询结果总记录数:"+searchHits.getTotalHits());
        //查询结果列表 (用迭代器的方式)
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit seatchHit = iterator.next();
            //getSourceAsString() 这样就可以把整个文档对象打印出来 就是一个json格式的字符串
            //打印文档对象 以json格式输出
            System.out.println(seatchHit.getSourceAsString());
            //取文档的属性
            System.out.println("-------文档的属性");
            Map<String, Object> document = seatchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }
        //关闭client
        client.close();
    }


    //通过id来查询
    @Test
    public void testSearchById() throws Exception {
        //创建一个client对象
        //创建一个查询对象
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1","2");
        search(queryBuilder);
    }

    //通过关键词来查询
    @Test
    public void testQueryByTerm() throws Exception {
        //创建一个QueryBuilder对象
        //参数1:要搜索的字段
        //参数2：要搜索的关键词
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title","北方");
        //执行查询
        search(queryBuilder);
    }

    @Test
    public void testQueryStringQuery() throws Exception {
        //创建一个QueryBuilder对象   设置默认的搜索域(不指定title的话会默认在所有域上进行查询)
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("我们的职责")
                .defaultField("title");
        //执行查询  指定高亮显示的字段为 title
        search(queryBuilder,"title");
    }

   //第二个参数是 高亮显示的域
    private void search(QueryBuilder queryBuilder, String highlightField) throws Exception {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //设置高亮显示的字段
        highlightBuilder.field(highlightField);
        //设置前缀
        highlightBuilder.preTags("<em>");
        //设置后缀
        highlightBuilder.postTags("</em>");
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                //设置分页信息  从0条开始  每页显示5行数据
                .setFrom(0)
                .setSize(5)
                //设置高亮信息
                .highlighter(highlightBuilder)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询结果总记录数:"+searchHits.getTotalHits());
        //查询结果列表 (用迭代器的方式)
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit seatchHit = iterator.next();
            //getSourceAsString() 这样就可以把整个文档对象打印出来 就是一个json格式的字符串
            //打印文档对象 以json格式输出
            System.out.println(seatchHit.getSourceAsString());
            //取文档的属性
            System.out.println("-------文档的属性");
            Map<String, Object> document = seatchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            System.out.println("************高亮结果");
            //取到高亮后的结果
            Map<String, HighlightField> highlightFields = seatchHit.getHighlightFields();
            System.out.println(highlightFields);
            //取title高亮显示的结果
            HighlightField field = highlightFields.get(highlightField);
            //取这里边高亮的结果
            Text[] fragments = field.getFragments();
            if(fragments != null){
                String title = fragments[0].toString();
                System.out.println(title);
            }
        }
        //关闭client
        client.close();
    }

}
