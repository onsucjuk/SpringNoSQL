package kopo.poly.persistance.mongodb.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import kopo.poly.dto.MelonDTO;
import kopo.poly.persistance.mongodb.AbstractMongoDBCommon;
import kopo.poly.persistance.mongodb.IMelonMapper;
import kopo.poly.util.CmmUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import org.bson.Document;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
    public List<MelonDTO> getSonList(String colNm) throws Exception {

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
}
