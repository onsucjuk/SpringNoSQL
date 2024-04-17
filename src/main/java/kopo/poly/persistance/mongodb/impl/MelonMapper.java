package kopo.poly.persistance.mongodb.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import kopo.poly.dto.MelonDTO;
import kopo.poly.persistance.mongodb.AbstractMongoDBCommon;
import kopo.poly.persistance.mongodb.IMelonMapper;
import kopo.poly.util.CmmUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import org.bson.Document;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class MelonMapper extends AbstractMongoDBCommon implements IMelonMapper {

    private final MongoTemplate mongodb;

    @Override
    public int insertSong(List<MelonDTO> pList, String colNm) throws Exception {

        log.info(this.getClass().getName() + ".insertSong Start!");

        int res = 0;

        if (pList == null) {
            pList = new LinkedList<>();
        }

        // 데이터를 저장할 컬렉션 생성
        super.createCollection(mongodb, colNm, "collectTime");

        // 저장할 컬렉션 객체 생성
        MongoCollection<Document> col = mongodb.getCollection(colNm);

        for (MelonDTO pDTO : pList) {
            // 레코드 한개씩 저장하기
            col.insertOne(new Document(new ObjectMapper().convertValue(pDTO, Map.class)));
        }

        res = 1;

        log.info(this.getClass().getName() + ".insertSong End!");

        return res;
    }

    @Override
    public List<MelonDTO> getSongList(String colNm) throws Exception {

        log.info(this.getClass().getName() + ".getSongList Start!");

        // 조회 결과를 전달하기 위한 객체 생성
        List<MelonDTO> rList = new LinkedList<>();

        MongoCollection<Document> col = mongodb.getCollection(colNm);

        // 조회 결과 중 출력할 컬럼들(SQL의 SELECT 절과 FROM절 가운데 컬럼들 과 유사)
        Document projection = new Document();
        projection.append("song", "$song");
        projection.append("singer", "$singer");

        // MongoDB는 무조건 ObjectID가 자동 생성, 조회 필요 없으면 비활성
        projection.append("_id", 0);

        // MongoDB의 find 명령어를 통해 조회할 경우 사용
        // 조회 데이터 양이 적으면 find[데이저 제한 16mb], 많으면 무조건 Aggregate
        FindIterable<Document> rs = col.find(new Document()).projection(projection);

        for (Document doc : rs) {
            String song = CmmUtil.nvl(doc.getString("song"));
            String singer = CmmUtil.nvl(doc.getString("singer"));

            log.info("song : " + song + "/ singer : " + singer);

            MelonDTO rDTO = MelonDTO.builder()
                    .song(song)
                    .singer(singer)
                    .build();

            rList.add(rDTO);
        }
        log.info(this.getClass().getName() + ".getSongList End!");

        return rList;
    }

    @Override
    public List<MelonDTO> getSingerSongCnt(String colNm) throws Exception {

        log.info(this.getClass().getName() + ".getSingerCnt Start!");

        // 조회 결과를 전달하기 위한 객체 생성하기
        List<MelonDTO> rList = new LinkedList<>();

        // MongoDB 조회 쿼리
        List<? extends Bson> pipeline = Arrays.asList(
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                    .append("singer", "$singer")
                                )
                                .append("COUNT(singer)", new Document()
                                        .append("$sum", 1)
                                )
                        ),
                new Document()
                        .append("$project", new Document()
                                .append("singer", "$_id.singer")
                                .append("singerCnt", "$COUNT(singer)")
                                .append("_id", 0)
                        ),
                new Document()
                        .append("$sort", new Document()
                                .append("singerCnt)", -1)
                        )
        );

        MongoCollection<Document> col = mongodb.getCollection(colNm);
        AggregateIterable<Document> rs = col.aggregate(pipeline).allowDiskUse(true);

        for (Document doc : rs) {
            String singer = doc.getString("singer");
            int singerCnt = doc.getInteger("singerCnt", 0);

            log.info("singer : " + singer + "/ singerCnt : " + singerCnt);

            MelonDTO rDTO = MelonDTO.builder()
                    .singer(singer)
                    .singerCnt(singerCnt)
                    .build();

            rList.add(rDTO);

            rDTO = null;
            doc = null;
        }

        rs = null;
        col = null;
        pipeline = null;

        log.info(this.getClass().getName() + ".getSingerSongCnt End!");

        return  rList;
    }

    @Override
    public List<MelonDTO> getSingerSong(String colNm, MelonDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".getSingerSong Start!");

        // 조회 결과를 전달하기 위한 객체 생성하기
        List<MelonDTO> rList = new LinkedList<>();

        MongoCollection<Document> col = mongodb.getCollection(colNm);

        // 조회할 조건(SQL의 WHERE 역할 / SELECT song, singer FROM MELON_20220321 where singer = '방탄소년단')
        Document query = new Document();
        query.append("singer", CmmUtil.nvl(pDTO.singer()));

        // 조회 결과 중 출력할 컬럼들(SQL의 SELECT절과 FROM절 가운데 컬럼들과 유사함)
        Document projection = new Document();
        projection.append("song", "$song");
        projection.append("singer", "$singer");

        // MongoDB는 무조건 ObjectId가 자동생성되며, ObjectID는 사용하지 않을 때, 조회할 필요가 없음
        // ObjectId를 가지고 오지 않을 때 사용함
        projection.append("_id", 0);

        // MongoDB의 find 명령어를 통해 조회할 경우 사용함
        // 조회하는 데이터의 양이 적은 경우, find를 사용하고, 데이터양이 많은 경우 무조건 Aggregate 사용한다.
        FindIterable<Document> rs = col.find(query).projection(projection);

        for (Document doc : rs) {

            String song = CmmUtil.nvl(doc.getString("song"));
            String singer = CmmUtil.nvl(doc.getString("singer"));

            MelonDTO rDTO = MelonDTO.builder()
                    .song(song)
                    .singer(singer)
                    .build();

            // 레코드 결과를 List에 저장하기
            rList.add(rDTO);

        }
        log.info(this.getClass().getName() + ".getSingerSong End!");

        return rList;
    }

    @Override
    public int dropCollection(String colNm) throws Exception {

        log.info(this.getClass().getName() + ".dropCollection Start!");

        int res = 0;

        super.dropCollection(mongodb, colNm);

        res = 1;


        log.info(this.getClass().getName() + ".dropCollection End!");

        return res;
    }

    @Override
    public int insertManyField(String colNm, List<MelonDTO> pList) throws Exception {

        log.info(this.getClass().getName() + ".insertManyField Start!");

        int res = 0;

        if (pList == null) {
            pList = new LinkedList<>();
        }

        // 데이터를 저장할 컬렉션 생성
        super.createCollection(mongodb, colNm, "collectTime");

        // 저장할 컬렉션 객체 생성
        MongoCollection<Document> col = mongodb.getCollection(colNm);

        List<Document> list = new ArrayList<>();

        // 람다식 활용 stream과 -> 사용
        pList.parallelStream().forEach(melon ->
                list.add(new Document(new ObjectMapper().convertValue(melon, Map.class))));

        col.insertMany(list);

        res = 1;

        log.info(this.getClass().getName() + ".insertManyField End!");

        return res;
    }

    @Override
    public int updateField(String colNm, MelonDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".updateField Start!");

        int res = 0;

        MongoCollection<Document> col = mongodb.getCollection(colNm);

        String singer = CmmUtil.nvl(pDTO.singer());
        String updateSinger = CmmUtil.nvl(pDTO.updateSinger());

        log.info("pColNm : " + colNm);
        log.info("singer : " + singer);
        log.info("updateSinger : " + updateSinger);

        // 조회할 조건(SQL의 WHERE 역할 / SELECT * FROM 컬렉션명 where singer='방탄소년단')
        Document query = new Document();
        query.append("singer", singer);

        // MongoDB 데이터 수정은 반드시 컬렉션을 조회하고, 조회된 ObjectID를 기반으로 데이터를 수정함
        // MongoDB 환경은 분산환경(Sharding)으로 구성될 수 있기 때문에 정확한 PK에 매칭하기 위해서임
        FindIterable<Document> rs = col.find(query);

        // 람다식 활용하여 컬렉션에 조회된 데이터들을 수정하기
        rs.forEach(doc -> col.updateOne(doc, new Document("$set", new Document("singer", updateSinger))));

        res = 1;

        log.info(this.getClass().getName() + ".updateField End!");

        return res;

    }

    @Override
    public List<MelonDTO> getUpdateSinger(String colNm, MelonDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".getUpdateSinger Start!");

        // 조회 결과를 전달하기 위한 객체 생성하기
        List<MelonDTO> rList = new LinkedList<>();

        MongoCollection<Document> col = mongodb.getCollection(colNm);

        // 조회할 조건(SQL의 WHERE 역할 / SELECT song, singer FROM 컬렉션명 where singer = '방탄소년단')
        Document query = new Document();
        query.append("singer", CmmUtil.nvl(pDTO.updateSinger()));

        // 조회 결과 중 출력할 컬럼들(SQL의 SELECT절과 FROM절 가운데 칼럼들과 유사함)
        Document projection = new Document();
        projection.append("song", "$song");
        projection.append("singer", "$singer");

        // MonogoDB는 무조건 ObjectId가 자동생성되며, ObjectID는  사용하지 않을 때, 조회할 필요가 없음
        // ObjectId를 가져오지 않을 때 사용
        projection.append("_id", 0);

        // MongoDB의 find 명령어를 통해 조회할 경우 사용
        // 조회하는 데이터의 양이 적은 경우, find를 사용, 데이터의 양이 많은 경우 Aggregate 사용
        FindIterable<Document> rs = col.find(query).projection(projection);

        for (Document doc : rs) {

            String song = CmmUtil.nvl(doc.getString("song"));
            String singer = CmmUtil.nvl(doc.getString("singer"));

            log.info("song : " + song + "/ singer : " + singer);

            MelonDTO rDTO = MelonDTO.builder()
                    .song(song)
                    .singer(singer)
                    .build();

            // 레코드 결과를 List에 저장하기
            rList.add(rDTO);

        }

        log.info(this.getClass().getName() + ".getUpdateSinger End!");

        return rList;
    }
}
