package kopo.poly.controller;

import kopo.poly.controller.response.CommonResponse;
import kopo.poly.dto.MelonDTO;
import kopo.poly.dto.MsgDTO;
import kopo.poly.service.IMelonService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@RequestMapping(value = "/melon/v1")
@RequiredArgsConstructor
@RestController
public class MelonController {

    private final IMelonService melonService;

    /**
     * 멜론 노래 리스트 저장하기
     */
    @PostMapping(value = "collectMelonSong")
    public ResponseEntity collectMelonSong() throws Exception {

        log.info(this.getClass().getName() + ".collectMelonSong Start!");

        // 수집 결과 출력
        String msg = "";

        int res = melonService.collectMelonSong();

        if (res == 1) {
            msg = "멜론차트 수집 성공!";
        } else {
            msg = "멜론차트 수집 실패!";
        }

        MsgDTO dto = MsgDTO.builder()
                .result(res)
                .msg(msg)
                .build();

        log.info(this.getClass().getName() + ".collectMelonSong End!");

        return ResponseEntity.ok(
                CommonResponse.of(HttpStatus.OK, HttpStatus.OK.series().name(), dto));
    }

    /**
     * 오늘 수집된 멜론 노래리스트 가져오기
     */
    @PostMapping(value = "getSongList")
    public ResponseEntity getSongList() throws Exception {

        log.info(this.getClass().getName() + ".getSongList Start!");

        // Java 8 부터 제공되는 Optional 활용하여 NPE(Null Pointer Exception) 처리
        List<MelonDTO> rList = Optional.ofNullable(melonService.getSongList())
                .orElseGet(ArrayList::new);

        log.info(this.getClass().getName() + ".getSongList End!");

        return ResponseEntity.ok(
                CommonResponse.of(HttpStatus.OK, HttpStatus.OK.series().name(), rList));
    }
}
