package kopo.poly.service.impl;

import kopo.poly.dto.RedisDTO;
import kopo.poly.persistance.redis.IMyRedisMapper;
import kopo.poly.service.IMyRedisService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
@Service
class MyRedisService implements IMyRedisService {

    private final IMyRedisMapper myRedisMapper;

    @Override
    public RedisDTO saveString(RedisDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".saveString Start!");

        // 저장할 RedisDB 키
        String redisKey = "myRedus_String";

        // 저장 결과
        RedisDTO rDTO = null;

        int res = myRedisMapper.saveString(redisKey, pDTO);

        if (res == 1) { // Redis 저장이 성공하면, 저장된 데이터 가져오기
            rDTO = myRedisMapper.getString(redisKey);
        } else {
            log.info("Redis 저장 실패!!");
            throw new Exception("Redis 저장 실패!!");
        }

        log.info(this.getClass().getName() + ".saveString End!");

        return rDTO;
    }

    @Override
    public RedisDTO saveStringJSON(RedisDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ". saveStringJSON Start!");

        // 저장할 RedisDB 키
        String redisKey = "myRedis_String_JSON";

        // 저장 결과
        RedisDTO rDTO = null;

        int res = myRedisMapper.saveStringJSON(redisKey, pDTO);

        if (res == 1) { // Redis 저장이 성공하면, 저장된 데이터 가져오기
            rDTO = myRedisMapper.getStringJSON(redisKey);

        } else {

            log.info("Redis 저장 실패 !!");
            throw new Exception("Redis 저장 실패 !!");

        }

        log.info(this.getClass().getName() + ".saveStringJSON End!");

        return rDTO;
    }

    @Override
    public List<String> saveList(List<RedisDTO> pList) throws Exception {

        log.info(this.getClass().getName() + ".saveList Start!");

        String redisKey = "myRedis_List";

        List<String> rList;

        int res = myRedisMapper.saveList(redisKey, pList);

        if (res == 1) {

            rList = myRedisMapper.getList(redisKey);

        } else {

            log.info("Redis 저장 실패!!");
            throw new Exception("Redis 저장 실패!!");

        }

        log.info(this.getClass().getName() + ".saveList End!");

        return rList;
    }

    @Override
    public List<RedisDTO> saveListJSON(List<RedisDTO> pList) throws Exception {

        log.info(this.getClass().getName() + ".saveListJSON Start!");

        String redisKey = "myRedis_List_JSON";

        List<RedisDTO> rList;

        int res = myRedisMapper.saveListJSON(redisKey, pList);

        if (res == 1) {

            rList = myRedisMapper.getListJSON(redisKey);

        } else {

            log.info("Redis 저장 실패!!");
            throw new Exception("Redis 저장 실패!!");

        }

        log.info(this.getClass().getName() + ".saveListJSON End!");

        return rList;
    }

    @Override
    public RedisDTO saveHash(RedisDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".saveHash Start!");

        String redisKey = "myRedis_Hash";

        RedisDTO rDTO;

        int res = myRedisMapper.saveHash(redisKey, pDTO);

        if (res == 1) {

            rDTO = myRedisMapper.getHash(redisKey);

        } else {

            log.info("Redis 저장 실패!!");
            throw new Exception("Redis 저장 실패!!");

        }

        log.info(this.getClass().getName() + ".saveHash End!");

        return rDTO;
    }

    @Override
    public Set<RedisDTO> saveSetJSON(List<RedisDTO> pList) throws Exception {

        log.info(this.getClass().getName() + ".saveSetJSON Start!");

        String redisKey = "myRedis_Set_JSON";

        Set<RedisDTO> rSet;

        int res = myRedisMapper.saveSetJSON(redisKey, pList);

        if (res == 1) {

            rSet = myRedisMapper.getSetJSON(redisKey);

        } else {

            log.info("Redis 저장 실패!!");
            throw new Exception("Redis 저장 실패!!");

        }

        log.info(this.getClass().getName() + ".saveSetJSON End!");

        return rSet;
    }

    @Override
    public Set<RedisDTO> saveZSetJSON(List<RedisDTO> pList) throws Exception {
        log.info(this.getClass().getName() + ".saveZSetJSON Start!");

        String redisKey = "myRedis_ZSet_JSON";

        Set<RedisDTO> rSet;

        int res = myRedisMapper.saveZSetJSON(redisKey, pList);

        if (res == 1) {

            rSet = myRedisMapper.getZSetJSON(redisKey);

        } else {

            log.info("Redis 저장 실패!!");
            throw new Exception("Redis 저장 실패!!");

        }

        log.info(this.getClass().getName() + ".saveZSetJSON End!");

        return rSet;
    }

}
