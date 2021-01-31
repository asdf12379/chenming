package com.itheima.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Set;

/**
 * @author chenming
 * @date 2020/10/25 -  10:55
 */
public class ElasticSearchClientTest {

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


    @Test
    public void creatIndex() throws Exception {
        // cluster:集群
        // 1.创建一个settings对象 相当于是一个配置信息 主要配置集群的名称
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();
        // 2.创建一个客户端client 对象
        TransportClient client = new PreBuiltTransportClient(settings);
        //指定集群中节点的列表  连接到InetAddress.getByName("127.0.0.1"),9301  这个节点
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        // 3.使用client对象创建一个索引库  hello
        client.admin().indices().prepareCreate("hello")
                //执行操作
                .get();
        // 4.关闭client对象
        client.close();
    }

    @Test
    public void setMappings() throws Exception{
        //创建一个settings对象
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();
        //创建一个TransPortClient对象
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        //创建一个Mappings信息
        /*{
            "article":{
            "properties":{
                "id":{
                    "type":"long",
                            "store":true
                },
                "title":{
                    "type":"text",
                            "store":true,
                            "index":true,
                            "analyzer":"ik_smart"
                },
                "content":{
                    "type":"text",
                            "store":true,
                            "index":true
                    "analyzer":"ik_smart"
                }
            }
        }
        }*/
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                        .startObject("acticle")
                             .startObject("properties")
                                   .startObject("id")
                                       .field("type","long")
                                       .field("store",true)
                                   .endObject()
                                   .startObject("title")
                                        .field("type","text")
                                        .field("store",true)
                                        .field("analyzer","ik_smart")
                                    .endObject()
                                    .startObject("content")
                                         .field("type","text")
                                         .field("store",true)
                                         .field("analyzer","ik_smart")
                                    .endObject()
                              .endObject()
                         .endObject()
                .endObject();
         //使用client把mapping信息设置到索引库中
        client.admin().indices()
                //设置要做映射的索引
                .preparePutMapping("hello")
                //设置要做映射的type
                .setType("acticle")
                //mapping信息 可以是XContentBuilder对象 可以是json格式的字符串
                .setSource(builder)
                //执行操作
                .get();
        //关闭链接
        client.close();
    }

    @Test
    public void testAddDocument() throws Exception {
        //创建一个client对象
        //创建一个文档对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id",2l)
                    .field("title","北方入秋速度明显加快222222")
                    .field("content","阿联酋一架客机")
                .endObject();
        //把文档对象添加到索引库
        client.prepareIndex()
                //设置索引名称
                .setIndex("hello")
                //设置type
                .setType("article")
                //设置文档的id,如果不设置的话会自动生成一个id
                .setId("2")
                //设置文档信息
                .setSource(builder)
                //执行操作
                .get();
        //关闭客户端
        client.close();
    }

    @Test
    public void testAddDocument2() throws Exception {
        //创建一个Article对象
        Article article = new Article();
        //设置对象的属性
        article.setId(3l);
        article.setTitle("调十颗卫星去拍摄");
        article.setContent("基辅级导弹驱逐舰");
        //把article对象转换成json格式的字符串
        ObjectMapper objectMapper = new ObjectMapper();
        //转成一个json串
        String jsonDocument = objectMapper.writeValueAsString(article);
        System.out.println(jsonDocument);
        //使用client对象把文档写入索引库
        client.prepareIndex("hello","article","3")
                //                 告诉他这是一个json格式的字符串
                .setSource(jsonDocument, XContentType.JSON)
                .get();
        //关闭客户端
        client.close();
    }

    @Test
    public void testAddDocument3() throws Exception {
        for (int i = 4; i < 100; i++) {
            //创建一个Article对象
            Article article = new Article();
            //设置对象的属性
            article.setId(i);
            article.setTitle("救人是职责更是本能"+i);
            article.setContent("多名官员被调查"+i);
            //把article对象转换成json格式的字符串
            ObjectMapper objectMapper = new ObjectMapper();
            //转成一个json串
            String jsonDocument = objectMapper.writeValueAsString(article);
            System.out.println(jsonDocument);
            //使用client对象把文档写入索引库
            client.prepareIndex("hello","article",i+"")
                    //                 告诉他这是一个json格式的字符串
                    .setSource(jsonDocument, XContentType.JSON)
                    .get();
        }
        //关闭客户端
        client.close();
    }





}
