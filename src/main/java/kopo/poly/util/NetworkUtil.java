package kopo.poly.util;

import org.springframework.lang.Nullable;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public class NetworkUtil {

    /**
     * @param apiUrl 호출할 OpenAPI URL주소
     */

    public static String get(String apiUrl) {
        return get(apiUrl, null);
    }

    /* 헤더값 존재 */

    public static String get(String apiUrl, @Nullable Map<String, String> requestHeaders) {
        HttpURLConnection con = connect(apiUrl);

        try {
            con.setRequestMethod("GET");

            // 전송할 헤더 값이 존재하면, 헤더 값 추가하기
            // 데이터 전송할 때, 헤더 정보의 추가가 필요할 때 사용
            if (requestHeaders != null) {
                for (Map.Entry<String, String> header : requestHeaders.entrySet()) {
                    con.setRequestProperty(header.getKey(), header.getValue());
                }
            }

            int responseCode = con.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                return readBody(con.getInputStream()); // 성공 결과 값을 문자로 반환하기
            } else { // 에러 발생
                return readBody(con.getErrorStream()); // 실패 결과 값을 문자로 반환하기
            }
        } catch (IOException e) {
            throw new RuntimeException("API 요청과 응답 실패", e);
        } finally {
            con.disconnect();
        }
    }

    /**
     * POST 방식
     *
     * @param apiUrl 호출할 OpenAPI URL 주소
     * @param postParams 전송할 파라미터
     * @param requestHeaders 전송하고 싶은 헤더 정보
     *
     */

    public static String post(String apiUrl, @Nullable Map<String, String> requestHeaders, String postParams) {
        HttpURLConnection con = connect(apiUrl);

        try {
            con.setRequestMethod("POST");

            // 전송할 헤더 값이 존재하면, 헤더 값 추가하기
            for (Map.Entry<String, String> header : requestHeaders.entrySet()) {
                con.setRequestProperty(header.getKey(), header.getValue());
            }

            // POST 방식으로 전송할 때, 전송할 파라미터 정보 넣기(GET 방식은 필요없음)
            con.setDoOutput(true);
            try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
                wr.write(postParams.getBytes());
                wr.flush();
            }

            // API 호출 후, 결과 받기
            int responseCode = con.getResponseCode();

            // API 호출 성공하면
            if (responseCode == HttpURLConnection.HTTP_OK) {
                return readBody(con.getInputStream());
            } else {
                return readBody(con.getErrorStream());
            }
        } catch (IOException e) {
            throw new RuntimeException("API 요청과 응답 실패", e);
        } finally {
            con.disconnect();
        }
    }

    /**
     *  OpenAPI URL에 접속하기
     *  이 함수는 NetworkUtil에서만 사용하기에 접근 제한자를 private로 선언
     *
     * @param apiUrl 호출할 OpenAPI URL 주소
     *
     */

    private static HttpURLConnection connect(String apiUrl) {
        try {
            URL url = new URL(apiUrl);

            return (HttpURLConnection) url.openConnection();

        } catch (MalformedURLException e) {
            throw new RuntimeException("API URL이 잘못되었습니다. : " + apiUrl, e);

        } catch (IOException e) {
            throw new RuntimeException("연결이 실패했습니다. : " + apiUrl, e);

        }
    }

    /**
     *
     * 받은 결과를 문자열로 변환하기
     *
     * @param body 읽은 결과값
     *
     */

    private static String readBody(InputStream body) {
        InputStreamReader streamReader = new InputStreamReader(body);

        //결과값 버퍼에 저장하기
        try (BufferedReader lineReader = new BufferedReader(streamReader)) {
            StringBuilder responseBody = new StringBuilder();

            String line;

            while ((line = lineReader.readLine()) != null) {
                responseBody.append(line);
            }

            return  responseBody.toString();
        } catch (IOException e) {
            throw  new RuntimeException("API 응답을 읽는데 실패했습니다.", e);
        }
    }










































}
