package com.msastudy.deadlocklab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DB 트랜잭션 데드락 재현 실습.
 *
 * <p>이 프로젝트가 실제로 쓰는 vehicle-db(Postgres)의 {@code vehicle} 테이블에서, 두 트랜잭션이
 * 같은 두 행(차량)을 {@code SELECT ... FOR UPDATE}로 잠그되 순서를 반대로 하면 어떻게 되는지
 * 직접 재현한다. JPA/Spring 없이 순수 JDBC로 작성했다 — 트랜잭션 락과 Postgres의 데드락 탐지를
 * 코드 레벨에서 직접 다루기 위함.
 *
 * <ul>
 *   <li>{@code broken} 모드: tx-1은 차량A→차량B 순서로, tx-2는 차량B→차량A 순서로 잠근다.
 *       CyclicBarrier로 "둘 다 자기 첫 번째 락을 잡을 때까지" 동기화한 다음 두 번째 락을
 *       동시에 시도하게 만들어서, 서로가 상대의 두 번째 락을 들고 서로를 기다리는 순환 대기
 *       (circular wait)를 확정적으로 만든다. Postgres가 기본 {@code deadlock_timeout}(1초) 뒤
 *       이를 탐지해 트랜잭션 하나를 {@code SQLState 40P01}로 강제 중단시킨다.</li>
 *   <li>{@code fixed} 모드: tx-1, tx-2 모두 차량A→차량B 순서로만 잠근다. 둘 다 같은 첫 번째
 *       자원을 먼저 요청하므로 하나가 먼저 잠그면 나머지는 그 자원 앞에서 그냥 "차례를
 *       기다리는" 상태가 된다(순환 대기 자체가 성립하지 않음) — 그래서 이 모드에서는
 *       broken 모드에서 쓴 barrier 동기화를 쓰지 않는다(쓰면 오히려 테스트 코드 자체가
 *       멈춘다: 뒤에 온 스레드가 첫 락을 못 잡아 barrier에 도착할 수 없기 때문).</li>
 * </ul>
 *
 * 실행: {@code ./gradlew :deadlock-lab:run --args="broken"} 또는 {@code --args="fixed"}
 * (docker-compose의 vehicle-db가 떠 있고, vehicle-service를 한 번 기동해서 재고가 시딩돼
 * 있어야 한다 — 잠글 행이 최소 2개 필요).
 */
public final class DeadlockLabRunner {

    private static final Logger log = LoggerFactory.getLogger(DeadlockLabRunner.class);

    private static final String JDBC_URL = "jdbc:postgresql://localhost:5433/vehicle";
    private static final String USER = "vehicle";
    private static final String PASSWORD = "vehicle";
    private static final String DEADLOCK_SQLSTATE = "40P01";

    private DeadlockLabRunner() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "broken";
        if (!mode.equals("broken") && !mode.equals("fixed")) {
            throw new IllegalArgumentException("mode는 broken 또는 fixed여야 한다: " + mode);
        }
        boolean broken = mode.equals("broken");

        List<String> ids = pickTwoVehicleIds();
        String vehicleA = ids.get(0);
        String vehicleB = ids.get(1);
        log.info("대상 차량: A={}, B={}", vehicleA, vehicleB);

        // broken 모드에서만 "둘 다 1차 락을 잡을 때까지" 동기화한다 (설계 이유는 클래스
        // 주석 참고). fixed 모드는 barrier 없이 그냥 동시에 시작한다.
        CyclicBarrier barrier = broken ? new CyclicBarrier(2) : null;

        Thread tx1 = new Thread(() -> runTransaction("tx-1", vehicleA, vehicleB, barrier), "tx-1");
        Thread tx2 = new Thread(
                () -> runTransaction("tx-2", broken ? vehicleB : vehicleA, broken ? vehicleA : vehicleB, barrier),
                "tx-2");

        long start = System.currentTimeMillis();
        tx1.start();
        tx2.start();
        tx1.join();
        tx2.join();
        long elapsed = System.currentTimeMillis() - start;

        log.info("==================== 결과 ({} mode) ====================", mode);
        log.info("전체 소요 시간: {}ms", elapsed);
        if (broken) {
            log.info("결론: 반대 순서로 잠갔기 때문에 순환 대기가 발생했고, Postgres가 데드락을 탐지해서" +
                    " 한쪽 트랜잭션을 강제로 중단시켰다 (위 로그의 'deadlock detected' 참고).");
        } else {
            log.info("결론: 항상 같은 순서(차량A -> 차량B)로 잠갔기 때문에 순환 대기 자체가 성립하지" +
                    " 않았다. 한쪽이 다른 쪽을 잠깐 기다리긴 했지만(정상적인 락 대기), 데드락 없이" +
                    " 둘 다 커밋에 성공했다.");
        }
    }

    private static List<String> pickTwoVehicleIds() throws SQLException {
        List<String> ids = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                PreparedStatement ps = conn.prepareStatement("SELECT id FROM vehicle ORDER BY id LIMIT 2");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
        }
        if (ids.size() < 2) {
            throw new IllegalStateException(
                    "vehicle 테이블에 행이 2개 이상 있어야 한다 (vehicle-service를 한 번 기동해서 재고를 시딩해야 함)");
        }
        return ids;
    }

    private static void runTransaction(String name, String firstId, String secondId, CyclicBarrier barrier) {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            conn.setAutoCommit(false);

            lockRow(conn, firstId);
            log.info("[{}] 1차 락 획득: {}", name, firstId);

            if (barrier != null) {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            log.info("[{}] 2차 락 시도: {}", name, secondId);
            long lockStart = System.currentTimeMillis();
            lockRow(conn, secondId);
            long lockElapsed = System.currentTimeMillis() - lockStart;
            log.info("[{}] 2차 락 획득: {} ({}ms 대기)", name, secondId, lockElapsed);

            conn.commit();
            log.info("[{}] 커밋 완료 - 정상 종료", name);
        } catch (SQLException e) {
            if (DEADLOCK_SQLSTATE.equals(e.getSQLState())) {
                log.warn("[{}] Postgres가 데드락을 탐지해서 이 트랜잭션을 강제 중단시킴: {}",
                        name, firstLine(e.getMessage()));
            } else {
                log.error("[{}] 예상치 못한 SQL 오류", name, e);
            }
        }
    }

    private static void lockRow(Connection conn, String vehicleId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM vehicle WHERE id = ? FOR UPDATE")) {
            ps.setString(1, vehicleId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
            }
        }
    }

    private static String firstLine(String message) {
        if (message == null) {
            return "";
        }
        int newline = message.indexOf('\n');
        return newline < 0 ? message : message.substring(0, newline);
    }
}
