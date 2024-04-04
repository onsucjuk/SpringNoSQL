package kopo.poly.service;

import kopo.poly.dto.MelonDTO;

import java.util.List;

public interface IMelonService {

    /**
     * 멜론 노래 리스트 저장하기
     */
    int collectMelonSong() throws Exception;

    /**
     * 오늘 수집된 멜론 노래리스트 가져오기
     */
    List<MelonDTO> getSongList() throws Exception;

}
