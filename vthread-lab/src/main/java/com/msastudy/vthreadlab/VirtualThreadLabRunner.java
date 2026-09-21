package com.msastudy.vthreadlab;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 가상 스레드(JDK 21) vs 플랫폼 스레드 벤치마크.
 *
 * <p>"Java의 블로킹 I/O 모델이 Node.js 같은 이벤트 루프 기반 모델보다 대량 동시 연결 처리에
 * 불리한가?"라는 질문에서 출발한 실습. 전통적인 Java는 요청 하나당 OS 스레드 하나를 점유하는
 * 플랫폼 스레드 모델이라, 블로킹 I/O(DB 조회, 외부 API 호출 등) 동안 그 스레드가 통째로
 * 묶여버린다 — 그래서 스레드 풀 크기(보통 수백 개)를 넘는 동시 요청은 무조건 대기열에서
 * 기다려야 했다. 가상 스레드는 블로킹 I/O로 멈추는 순간 OS 스레드(캐리어 스레드)를 반납하고,
 * 수만 개를 만들어도 실제 OS 스레드는 CPU 코어 수만큼만 쓰기 때문에 이 병목이 사라진다 —
 * 단, 이건 "블로킹 I/O로 기다리는 작업"에만 해당되고 "CPU를 실제로 쓰는 연산"에는 아무 도움이
 * 안 된다는 것까지 같이 보여준다.
 *
 * <p>Spring/Tomcat 없이 순수 {@code java.util.concurrent}만 쓴다 - 프레임워크 레이어(커넥션 풀,
 * 톰캣 스레드 풀 설정 등)를 끼우면 "가상 스레드 자체의 차이"를 보기 어려워지기 때문.
 *
 * <p>실행: {@code ./gradlew :vthread-lab:run}
 */
public final class VirtualThreadLabRunner {

    // I/O-bound 시나리오: DB/외부 API 호출 한 번을 50ms짜리 블로킹으로 흉내낸다.
    private static final int IO_TASK_COUNT = 10_000;
    private static final int IO_SLEEP_MS = 50;

    // CPU-bound 시나리오: 진짜 CPU 연산(소수 개수 세기, 시행 나눗셈이라 일부러 비효율적으로 무거움).
    private static final int CPU_TASK_COUNT = 2_000;
    private static final int CPU_PRIME_BOUND = 30_000;

    // Spring Boot 내장 Tomcat의 기본 server.tomcat.threads.max(200)와 동일하게 맞춰서,
    // "실제 서비스에 흔히 쓰이는 설정"과 비교되게 했다.
    private static final int PLATFORM_POOL_SIZE = 200;

    private VirtualThreadLabRunner() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("CPU 코어 수(availableProcessors): " + Runtime.getRuntime().availableProcessors());
        System.out.println();

        System.out.println("==================== I/O-bound (작업당 " + IO_SLEEP_MS + "ms 블로킹 sleep, "
                + IO_TASK_COUNT + "개 작업) ====================");
        long ioPlatformMs = runIoBound(newPlatformExecutor(), "플랫폼 스레드 (풀 크기 " + PLATFORM_POOL_SIZE + ")");
        long ioVirtualMs = runIoBound(Executors.newVirtualThreadPerTaskExecutor(), "가상 스레드 (작업당 1개)");

        System.out.println();
        System.out.println("==================== CPU-bound (작업당 " + CPU_PRIME_BOUND + " 이하 소수 개수 세기, "
                + CPU_TASK_COUNT + "개 작업) ====================");
        long cpuPlatformMs = runCpuBound(newPlatformExecutor(), "플랫폼 스레드 (풀 크기 " + PLATFORM_POOL_SIZE + ")");
        long cpuVirtualMs = runCpuBound(Executors.newVirtualThreadPerTaskExecutor(), "가상 스레드 (작업당 1개)");

        System.out.println();
        System.out.println("==================== 결과 요약 ====================");
        System.out.printf("I/O-bound  : 플랫폼 %,dms  vs  가상 %,dms  (%.1f배)%n",
                ioPlatformMs, ioVirtualMs, ioPlatformMs / (double) Math.max(ioVirtualMs, 1));
        System.out.printf("CPU-bound  : 플랫폼 %,dms  vs  가상 %,dms  (%.2f배)%n",
                cpuPlatformMs, cpuVirtualMs, cpuPlatformMs / (double) Math.max(cpuVirtualMs, 1));
    }

    private static ExecutorService newPlatformExecutor() {
        return Executors.newFixedThreadPool(PLATFORM_POOL_SIZE);
    }

    private static long runIoBound(ExecutorService executor, String label) throws InterruptedException {
        List<Callable<Void>> tasks = new ArrayList<>(IO_TASK_COUNT);
        for (int i = 0; i < IO_TASK_COUNT; i++) {
            tasks.add(() -> {
                Thread.sleep(IO_SLEEP_MS);
                return null;
            });
        }

        long start = System.nanoTime();
        try (executor) {
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        System.out.printf("[%s] %,d개 작업 완료, 총 %,dms 소요%n", label, IO_TASK_COUNT, elapsedMs);
        return elapsedMs;
    }

    private static long runCpuBound(ExecutorService executor, String label) throws InterruptedException {
        List<Callable<Integer>> tasks = new ArrayList<>(CPU_TASK_COUNT);
        for (int i = 0; i < CPU_TASK_COUNT; i++) {
            tasks.add(() -> countPrimesBelow(CPU_PRIME_BOUND));
        }

        AtomicLong sanitySum = new AtomicLong();
        long start = System.nanoTime();
        try (executor) {
            List<Future<Integer>> futures = executor.invokeAll(tasks);
            for (Future<Integer> f : futures) {
                try {
                    sanitySum.addAndGet(f.get());
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        // JIT이 결과를 안 쓰는 계산이라고 판단해서 통째로 스킵(dead code elimination)해버리는 걸
        // 막기 위해 합계를 실제로 출력한다 - 매번 같은 값이 나와야 "진짜로 다 계산했다"는 증거다.
        System.out.printf("[%s] %,d개 작업 완료, 총 %,dms 소요 (sanity sum=%,d)%n",
                label, CPU_TASK_COUNT, elapsedMs, sanitySum.get());
        return elapsedMs;
    }

    /** 일부러 비효율적인 시행 나눗셈 - 진짜 CPU를 태우는 작업이 필요해서 최적화하지 않았다. */
    private static int countPrimesBelow(int bound) {
        int count = 0;
        for (int n = 2; n < bound; n++) {
            boolean prime = true;
            for (int d = 2; d * d <= n; d++) {
                if (n % d == 0) {
                    prime = false;
                    break;
                }
            }
            if (prime) {
                count++;
            }
        }
        return count;
    }
}
