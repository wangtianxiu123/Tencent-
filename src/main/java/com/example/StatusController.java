package com.example;

import com.google.gson.JsonObject;
import com.tencent.tcvdbtext.encoder.SparseVectorBm25Encoder;
import com.tencent.tcvectordb.client.VectorDBClient;
import com.tencent.tcvectordb.examples.CommonService;
import com.tencent.tcvectordb.model.Collection;
import com.tencent.tcvectordb.model.Database;
import com.tencent.tcvectordb.model.DocField;
import com.tencent.tcvectordb.model.Document;
import com.tencent.tcvectordb.model.param.collection.*;
import com.tencent.tcvectordb.model.param.database.ConnectParam;
import com.tencent.tcvectordb.model.param.dml.*;
import com.tencent.tcvectordb.model.param.entity.AffectRes;
import com.tencent.tcvectordb.model.param.enums.EmbeddingModelEnum;
import com.tencent.tcvectordb.model.param.enums.ReadConsistencyEnum;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.util.*;

import static com.tencent.tcvectordb.model.param.enums.EmbeddingModelEnum.BGE_BASE_ZH;

/**
 * @author gaolei
 */
@RestController
@Slf4j
@RequiredArgsConstructor
public class StatusController {

    private static final Map<String, String> FILE_READ_MAP = new LinkedHashMap<String, String>();

    private static final String DBNAME = "book_5";
    private static final String COLL_NAME = "book_segments_sparse_4";
    private static final String COLL_NAME_ALIAS = "collection_alias_sparse_4";

    public static void main(String[] args) throws InterruptedException {

    }


    private static void createDatabaseAndCollection(VectorDBClient client) {
        // 1. 创建数据库
        System.out.println("---------------------- createDatabase ----------------------");
        Database db = client.createDatabase(DBNAME);

        // 2. 列出所有数据库
        System.out.println("---------------------- listCollections ----------------------");
        List<String> database = client.listDatabase();
        for (String s : database) {
            System.out.println("\tres: " + s);
        }
//        Database db = client.database(DBNAME);

        // 3. 创建 collection
        System.out.println("---------------------- createCollection ----------------------");
        CreateCollectionParam collectionParam = initCreateCollectionParam(COLL_NAME);
        db.createCollection(collectionParam);

        // 4. 列出所有 collection
//        Database db = client.database(DBNAME);
        System.out.println("---------------------- listCollections ----------------------");
        List<Collection> cols = db.listCollections();
        for (Collection col : cols) {
            System.out.println("\tres: " + col.toString());
        }

        // 5. 设置 collection 别名
        System.out.println("---------------------- setAlias ----------------------");
        AffectRes affectRes = db.setAlias(COLL_NAME, COLL_NAME_ALIAS);
        System.out.println("\tres: " + affectRes.toString());


        // 6. describe collection
        System.out.println("---------------------- describeCollection ----------------------");
        Collection descCollRes = db.describeCollection(COLL_NAME);
        System.out.println("\tres: " + descCollRes.toString());

        // 7. delete alias
        System.out.println("---------------------- deleteAlias ----------------------");
        AffectRes affectRes1 = db.deleteAlias(COLL_NAME_ALIAS);
        System.out.println("\tres: " + affectRes1);

        // 8. describe collection
        System.out.println("---------------------- describeCollection ----------------------");
        Collection descCollRes1 = db.describeCollection(COLL_NAME);
        System.out.println("\tres: " + descCollRes1.toString());

    }

    private static List<Double> generateRandomVector(int dim) {
        Random random = new Random();
        List<Double> vectors = new ArrayList<>();

        for (int i = 0; i < dim; i++) {
            double randomDouble = 0 + random.nextDouble() * (1.0 - 0.0);
            vectors.add(randomDouble);
        }
        return vectors;
    }

    private static void upsertData(VectorDBClient client) throws InterruptedException {
        Database database = client.database(DBNAME);
        Collection collection = database.describeCollection(COLL_NAME);
        SparseVectorBm25Encoder encoder = SparseVectorBm25Encoder.getBm25Encoder("zh");
        List<String> texts = Arrays.asList(
                "富贵功名，前缘分定，为人切莫欺心。",
                "正大光明，忠良善果弥深。些些狂妄天加谴，眼前不遇待时临。",
                "细作探知这个消息，飞报吕布。",
                "布大惊,与陈宫商议.宫曰:\"闻刘玄德新领徐州,可往投之.\"布从其言,竟投徐州来.有人报知玄德.",
                "玄德曰:\"布乃当今英勇之士,可出迎之.\"糜竺曰:\"吕布乃虎狼之徒,不可收留;收则伤人矣.\""
        );

        List<List<Pair<Long, Float>>> sparseVectors = encoder.encodeTexts(texts);
        List<Document> documentList = new ArrayList<>(Arrays.asList(
                Document.newBuilder()
                        .withId("0001")
//                        .withVector(generateRandomVector(768))
//                        .withVectorByEmbeddingText("富贵功名，前缘分定，为人切莫欺心。")
                        .withSparseVector(sparseVectors.get(0))
                        .addDocField(new DocField("bookName", "三国演义"))
                        .addDocField(new DocField("author", "罗贯中"))
                        .addDocField(new DocField("page", 21))
                        .addDocField(new DocField("segment", "富贵功名，前缘分定，为人切莫欺心。"))
                        .addDocField(new DocField("text", "富贵功名，前缘分定，为人切莫欺心。"))
                        .build(),
                Document.newBuilder()
                        .withId("0002")
//                        .withVectorByEmbeddingText("正大光明，忠良善果弥深。些些狂妄天加谴，眼前不遇待时临。")
//                        .withVector(generateRandomVector(768))
                        .withSparseVector(sparseVectors.get(1))
                        .addDocField(new DocField("bookName", "三国演义"))
                        .addDocField(new DocField("author", "罗贯中"))
                        .addDocField(new DocField("page", 22))
                        .addDocField(new DocField("segment",
                                "正大光明，忠良善果弥深。些些狂妄天加谴，眼前不遇待时临。"))
                        .addDocField(new DocField("text",
                                "正大光明，忠良善果弥深。些些狂妄天加谴，眼前不遇待时临。"))
                        .build(),
                Document.newBuilder()
                        .withId("0003")
//                        .withVector(generateRandomVector(768))
//                        .withVectorByEmbeddingText("细作探知这个消息，飞报吕布。")
                        .withSparseVector(sparseVectors.get(2))
                        .addDocField(new DocField("bookName", "三国演义"))
                        .addDocField(new DocField("author", "罗贯中"))
                        .addDocField(new DocField("page", 23))
                        .addDocField(new DocField("segment", "细作探知这个消息，飞报吕布。"))
                        .addDocField(new DocField("text", "细作探知这个消息，飞报吕布。"))
                        .build(),
                Document.newBuilder()
                        .withId("0004")
//                        .withVector(generateRandomVector(768))
//                        .withVectorByEmbeddingText("富贵功名，前缘分定，为人切莫欺心。")
                        .withSparseVector(sparseVectors.get(3))
                        .addDocField(new DocField("bookName", "三国演义"))
                        .addDocField(new DocField("author", "罗贯中"))
                        .addDocField(new DocField("page", 24))
                        .addDocField(new DocField("segment", "布大惊,与陈宫商议.宫曰:\"闻刘玄德新领徐州,可往投之.\"布从其言,竟投徐州来.有人报知玄德."))
                        .addDocField(new DocField("text", "布大惊,与陈宫商议.宫曰:\"闻刘玄德新领徐州,可往投之.\"布从其言,竟投徐州来.有人报知玄德."))
                        .build(),
                Document.newBuilder()
                        .withId("0005")
//                        .withVector(generateRandomVector(768))
                        .withSparseVector(sparseVectors.get(4))
//                        .withVectorByEmbeddingText("布大惊，与陈宫商议。宫曰："闻刘玄德新领徐州，可往投之。")
                        .addDocField(new DocField("bookName", "三国演义"))
                        .addDocField(new DocField("author", "罗贯中"))
                        .addDocField(new DocField("page", 25))
                        .addDocField(new DocField("segment",
                                "玄德曰:\"布乃当今英勇之士,可出迎之.\"糜竺曰:\"吕布乃虎狼之徒,不可收留;收则伤人矣.\""))
                        .addDocField(new DocField("text",
                                "玄德曰:\"布乃当今英勇之士,可出迎之.\"糜竺曰:\"吕布乃虎狼之徒,不可收留;收则伤人矣.\""))
                        .build()));
        System.out.println("---------------------- upsert ----------------------");
        InsertParam insertParam = InsertParam.newBuilder()
                .addAllDocument(documentList)
                .withBuildIndex(true)
                .build();
        collection.upsert(insertParam);

        // notice：upsert操作可用会有延迟
        Thread.sleep(1000 * 3);
    }

    private void queryData(VectorDBClient client) throws IOException {
        Database database = client.database(DBNAME);
        Collection collection = database.describeCollection(COLL_NAME);

        // query  查询
        // 1. query 用于查询数据
        // 2. 可以通过传入主键 id 列表或 filter 实现过滤数据的目的
        // 3. 如果没有主键 id 列表和 filter 则必须传入 limit 和 offset，类似 scan 的数据扫描功能
        // 4. 如果仅需要部分 field 的数据，可以指定 output_fields 用于指定返回数据包含哪些 field，不指定默认全部返回

        System.out.println("---------------------- query ----------------------");
        List<String> documentIds = Arrays.asList("0001", "0002", "0003", "0004", "0005");
        Filter filterParam = new Filter("bookName=\"三国演义\"");
        List<String> outputFields = Arrays.asList("id", "bookName", "segment");
        QueryParam queryParam = QueryParam.newBuilder()
                .withDocumentIds(documentIds)
                // 使用 filter 过滤数据
                .withFilter(filterParam)
                // limit 限制返回行数，1 到 16384 之间
                .withLimit(2)
                // 偏移
                .withOffset(1)
                // 指定返回的 fields
                .withOutputFields(outputFields)
                // 是否返回 vector 数据
                .withRetrieveVector(false)
                .build();
        List<Document> qdos = collection.query(queryParam);
        for (Document doc : qdos) {
            System.out.println("\tres: " + doc.toString());
        }

        // search稀疏向量搜索和向量搜索混合
//        List<Double> vector = Arrays.asList(0.011228000745177269,...);

        System.out.println("---------------------- hybridSearch ----------------------");
        SparseVectorBm25Encoder encoder = SparseVectorBm25Encoder.getBm25Encoder("zh");
        HybridSearchParam hybridSearchParam = HybridSearchParam.newBuilder()
                .withAnn(AnnOption.newBuilder().withFieldName("text")
                        .withTextData("凶狠残暴的人")
//                        .withData(generateRandomVector(768))
                        .build())
                .withMatch(MatchOption.newBuilder().withFieldName("sparse_vector")
                        .withData(encoder.encodeQueries(Arrays.asList("凶狠残暴的人")))
                        .build())
                // 指定 Top K 的 K 值
                .withRerank(new WeightRerankParam(Arrays.asList("vector", "sparse_vector"), Arrays.asList(1, 0)))
                .withLimit(10)
                // 过滤获取到结果
                .withFilter(filterParam)
//                .withRetrieveVector(true)
//                .withOutputFields(Arrays.asList("segment"))
                .build();
//        List<List<Document>> siDocs = collection.hybridSearch(hybridSearchParam).getDocumentsList();
        List<Document> docs = collection.hybridSearch(hybridSearchParam).getDocuments();
//        int i = 0;
//        for (Object docs : siDocs) {
////            System.out.println("\tres: " + (i++) + docs.toString());
//            for (Document doc : (List<Document>) docs) {
//                System.out.println("\tres: " + doc.toString());
//            }
//        }
        for (Document doc : docs) {
            System.out.println("\tres 1111111111111111111111: " + doc.toString());
        }
    }

    private static void updateAndDelete(VectorDBClient client) throws InterruptedException {
        Database database = client.database(DBNAME);
        Collection collection = database.describeCollection(COLL_NAME);


        // update
        // 1. update 提供基于 [主键查询] 和 [Filter 过滤] 的部分字段更新或者非索引字段新增

        // filter 限制仅会更新 id = "0003"
        System.out.println("---------------------- update ----------------------");
        Filter filterParam = new Filter("bookName=\"三国演义\"");
        List<String> documentIds = Arrays.asList("0001", "0003");
        SparseVectorBm25Encoder encoder = SparseVectorBm25Encoder.getBm25Encoder("zh");
        UpdateParam updateParam = UpdateParam
                .newBuilder()
                .addAllDocumentId(documentIds)
                .withFilter(filterParam)
                .build();
        Document updateDoc = Document
                .newBuilder()
                .addDocField(new DocField("page", 100))
                // 支持添加新的内容
                .addDocField(new DocField("extend", "extendContent"))
                .withSparseVector(encoder.encodeQueries(Arrays.asList("正大光明，忠良善果弥深")).get(0))
                .build();
        collection.update(updateParam, updateDoc);

        // delete
        // 1. delete 提供基于[ 主键查询]和[Filter 过滤]的数据删除能力
        // 2. 删除功能会受限于 collection 的索引类型，部分索引类型不支持删除操作

        // filter 限制只会删除 id = "00001" 成功
        System.out.println("---------------------- delete ----------------------");
        filterParam = new Filter("bookName=\"西游记\"");
        DeleteParam build = DeleteParam
                .newBuilder()
                .addAllDocumentId(documentIds)
                .withFilter(filterParam)
                .build();
        collection.delete(build);

        // notice：delete操作可用会有延迟
        Thread.sleep(1000 * 5);

        // rebuild index
        System.out.println("---------------------- rebuild index ----------------------");
        RebuildIndexParam rebuildIndexParam = RebuildIndexParam
                .newBuilder()
                .withDropBeforeRebuild(false)
                .withThrottle(1)
                .build();
        collection.rebuildIndex(rebuildIndexParam);
        Thread.sleep(5 * 1000);


        // truncate 会清除整个 Collection 的数据，包括索引
        System.out.println("---------------------- truncate collection ----------------------");
        AffectRes affectRes = database.truncateCollections(COLL_NAME);
        System.out.println("\tres: " + affectRes.toString());

        Thread.sleep(5 * 1000);
    }

    private static void deleteAndDrop(VectorDBClient client) {
        Database database = client.database(DBNAME);

        // 删除 collection
        System.out.println("---------------------- dropCollection ----------------------");
        database.dropCollection(COLL_NAME);

        // 删除 database
        System.out.println("---------------------- dropDatabase ----------------------");
        client.dropDatabase(DBNAME);
    }


    private static void clear(VectorDBClient client) {
//        List<String> databases = client.listDatabase();
//        for (String database : databases) {
//            client.dropDatabase(database);
//        }
        client.dropDatabase(DBNAME);
    }


    /**
     * 初始化创建 Collection 参数
     * 通过调用 addField 方法设计索引（不是设计 Collection 的结构）
     * <ol>
     *     <li>【重要的事】向量对应的文本字段不要建立索引，会浪费较大的内存，并且没有任何作用。</li>
     *     <li>【必须的索引】：主键id、向量字段 vector、稀疏向量sparse_vector 这两个字段目前是固定且必须的，参考下面的例子；</li>
     *     <li>【其他索引】：检索时需作为条件查询的字段，比如要按书籍的作者进行过滤，这个时候author字段就需要建立索引，
     *     否则无法在查询的时候对 author 字段进行过滤，不需要过滤的字段无需加索引，会浪费内存；</li>
     *     <li>向量数据库支持动态 Schema，写入数据时可以写入任何字段，无需提前定义，类似MongoDB.</li>
     *     <li>例子中创建一个书籍片段的索引，例如书籍片段的信息包括 {id, vector, segment, bookName, author, page},
     *     id 为主键需要全局唯一，segment 为文本片段, vector 字段需要建立向量索引，假如我们在查询的时候要查询指定书籍
     *     名称的内容，这个时候需要对 bookName 建立索引，其他字段没有条件查询的需要，无需建立索引。/li>
     *     </li>
     * </ol>
     *
     * @param collName
     * @return
     */
    private static CreateCollectionParam initCreateCollectionParam(String collName) {
        return CreateCollectionParam.newBuilder()
                .withName(collName)
                .withShardNum(1)
                .withReplicaNum(0)
                .withDescription("test sparse collection0")
                .addField(new FilterIndex("id", FieldType.String, IndexType.PRIMARY_KEY))
                .addField(new VectorIndex("vector", BGE_BASE_ZH.getDimension(), IndexType.HNSW,
                        MetricType.IP, new HNSWParams(16, 200)))
                .addField(new SparseVectorIndex("sparse_vector", IndexType.INVERTED, MetricType.IP))
                .addField(new FilterIndex("bookName", FieldType.String, IndexType.FILTER))
                .addField(new FilterIndex("author", FieldType.String, IndexType.FILTER))
                .withEmbedding(com.tencent.tcvectordb.model.param.collection.Embedding.newBuilder().withVectorField("vector").
                        withField("text").withModelName(EmbeddingModelEnum.BGE_BASE_ZH.getModelName()).build())
                .withFilterIndexConfig(FilterIndexConfig.newBuilder().withFilterAll(true).build())
                .build();
    }

    @GetMapping("/testVector")
    public String testVector(@RequestParam(required = false) String inputValue) throws InterruptedException, IOException {
        // 创建VectorDB Client
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUrl("http://lb-o1jzbc6h-rmkf8txyz0c17dq5.clb.ap-shanghai.tencentclb.com:10000")
                .withUsername("root")
                .withKey("R4fThwO4VJhgp9m70YvKDprZ8IrIuzBi1b43Gb9i")
                .withTimeout(600)
                .withConnectTimeout(600)
                .build();
        VectorDBClient client = new VectorDBClient(connectParam, ReadConsistencyEnum.EVENTUAL_CONSISTENCY);

        // 清理环境
        CommonService.anySafe(() -> client.dropDatabase(DBNAME));

        // 测试
        createDatabaseAndCollection(client);
        upsertData(client);
        queryData(client);
        
        // 在删除数据之前执行 hybridSearch
        StringBuilder result = new StringBuilder();
        try {
            // 构造 hybridSearch 参数
            HybridSearchParam hybridSearchParam = HybridSearchParam.newBuilder()
                    .withAnn(AnnOption.newBuilder().withFieldName("text")
                            .withTextData(inputValue != null ? inputValue : "凶狠残暴的人")
                            .build())
                    .withMatch(MatchOption.newBuilder().withFieldName("sparse_vector")
                            .withData(SparseVectorBm25Encoder.getBm25Encoder("zh").encodeQueries(Arrays.asList(inputValue != null ? inputValue : "凶狠残暴的人")))
                            .build())
                    // 指定 Top K 的 K 值
                    .withRerank(new WeightRerankParam(Arrays.asList("vector", "sparse_vector"), Arrays.asList(0, 1)))
                    .withLimit(10)
                    // 过滤获取到结果
                    .withFilter(new Filter("bookName=\"三国演义\""))
                    .build();
            
            // 执行搜索并获取结果
            List<Document> docs = client.database(DBNAME).describeCollection(COLL_NAME).hybridSearch(hybridSearchParam).getDocuments();
            
            // 将结果存入 StringBuilder
            result.append("<style>\n")
                .append("h2 { margin: 0 0 10px 0; color: #333; }\n")
                .append("table { width: 100%; border-collapse: collapse; margin-top: 10px; }\n")
                .append("th, td { padding: 10px 15px; text-align: left; border-bottom: 1px solid #ddd; }\n")
                .append("th { background-color: #4CAF50; color: white; }\n")
                .append("tr:hover { background-color: #f5f5f5; }\n")
                .append(".search-container { margin-top: 0; }\n")
                .append("</style>\n");
            
            result.append("<div class='search-container'>\n");
            result.append("<h2>搜索结果（按相关度排序）：</h2>\n");
            
            // 开始表格
            result.append("<table>\n")
                .append("<thead>\n")
                .append("<tr>")
                .append("<th>相关度分数</th>")
                .append("<th>内容</th>")
                .append("</tr>\n")
                .append("</thead>\n")
                .append("<tbody>\n");
            
            for (Document doc : docs) {
                Map<String, Object> fields = new HashMap<>();
                for (DocField field : doc.getDocFields()) {
                    fields.put(field.getName(), field.getValue());
                }
                result.append("<tr>\n")
                    .append("<td>").append(String.format("%.2f", doc.getScore())).append("</td>\n")
                    .append("<td>").append(fields.get("text")).append("</td>\n")
                    .append("</tr>\n");
            }
            
            // 结束表格和容器
            result.append("</tbody>\n")
                .append("</table>\n")
                .append("</div>\n");
        } finally {
            // 无论搜索是否成功，都要执行清理操作
            updateAndDelete(client);
            deleteAndDrop(client);
        }
        
        return result.toString();
    }
}
