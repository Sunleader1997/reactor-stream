package io.github.sunleader1997.reactorstream.abs;

import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

public class BlockingEmitFailureHandler implements Sinks.EmitFailureHandler {

      private final long sleepNanos;

      /**
       * @param sleepStep  每次重试间隔
       */
      public BlockingEmitFailureHandler(Duration sleepStep) {
          this.sleepNanos = sleepStep.toNanos();
      }

      public static BlockingEmitFailureHandler wait(Duration sleepStep){
          return new BlockingEmitFailureHandler(sleepStep);
      }

      @Override
      public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
          // 让出 CPU，等待一小段时间再重试
          LockSupport.parkNanos(sleepNanos);
          return true;
      }
  }