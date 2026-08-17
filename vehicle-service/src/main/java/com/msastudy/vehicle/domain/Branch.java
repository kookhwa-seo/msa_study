package com.msastudy.vehicle.domain;

/**
 * 실제 차량이 배치될 수 있는 지점(법정동 단위) 하나. DB에 저장하지 않는 순수 참조 데이터로,
 * {@code branches.json}(국토교통부 전국 법정동 데이터를 서울/부산/인천/대구/대전/광주/울산/세종
 * 8개 광역시로 필터링한 것)에서 그대로 읽어온다. {@code code}(법정동코드)가 곧 예약/차량이
 * 사용하는 {@code branchId}다.
 */
public record Branch(String code, String sido, String sigungu, String dong) {
}
