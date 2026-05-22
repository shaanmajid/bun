import { expect, test } from "bun:test";
import { bunEnv, bunExe, isASAN, isDebug, isWindows } from "harness";

// Worker VM startup/teardown is much slower under debug and/or ASAN; these
// tests spawn many workers, so scale iteration counts and timeouts down.
// ASAN catches the underlying UAF deterministically, so fewer iterations
// are still sufficient regression coverage.
const slow = isDebug || isASAN;
const rounds = slow ? 4 : 8;
const perRound = slow ? 12 : 32;
const timeout = slow ? 60_000 : 20_000;

// Regression: `new Worker(url, { ref: false })` was silently ignored — the
// Zig-side `user_keep_alive` field was set from it but never read, and the
// parent keep-alive was taken unconditionally in `create()`. `.unref()` after
// construction worked; the constructor option did not.
test("new Worker with { ref: false } does not keep the parent alive", async () => {
  await using proc = Bun.spawn({
    cmd: [
      bunExe(),
      "-e",
      `
        // The worker never exits on its own; if ref:false is honoured the
        // parent process exits immediately after spawning it.
        new Worker("data:text/javascript,setInterval(() => {}, 100000)", { ref: false });
        console.log("spawned");
      `,
    ],
    env: bunEnv,
    stdout: "pipe",
    stderr: "pipe",
  });

  const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
  expect(stderr).toBe("");
  expect(stdout).toBe("spawned\n");
  expect(exitCode).toBe(0);
});

// Regression: the Zig WebWorker struct was freed by the worker thread in
// exitAndDeinit while the C++ Worker still held a raw impl_ pointer, so a
// terminate()/ref()/unref() that landed after natural exit dereferenced freed
// memory (ASAN use-after-poison in setRefInternal).
test(
  "terminate/ref/unref after worker exits naturally does not UAF",
  async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `
        for (let round = 0; round < ${rounds}; round++) {
          const workers = [];
          for (let i = 0; i < ${perRound}; i++) {
            // Empty body: worker thread exits as soon as the event loop drains.
            workers.push(new Worker("data:text/javascript,"));
          }
          await Promise.all(workers.map(w => new Promise(r => w.addEventListener("close", r, { once: true }))));
          // All workers have exited; previously the Zig struct was freed here,
          // so every call below dereferenced freed memory via Worker::impl_.
          for (const w of workers) {
            w.ref();
            w.unref();
            w.terminate();
            w.terminate();
            w.ref();
            w.unref();
          }
        }
      `,
      ],
      env: bunEnv,
      stdout: "pipe",
      stderr: "pipe",
    });

    const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
    expect(stderr).toBe("");
    expect(stdout).toBe("");
    expect(exitCode).toBe(0);
  },
  timeout,
);

// Regression: WebWorker__dispatchExit deref'd the C++ Worker on the worker
// thread; if that was the last ref, ~Worker → ~EventTarget ran there and
// EventListenerMap::releaseAssertOrSetThreadUID tripped because the listener
// map was populated on the parent thread.
test(
  "nested worker whose grandchild outlives the middle worker's JSWorker does not assert",
  async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `
        for (let i = 0; i < ${rounds}; i++) {
          const middle = new Worker(
            'data:text/javascript,' +
            // Middle worker creates an inner worker, registers a listener (so the
            // inner Worker's EventListenerMap is tagged with the middle thread),
            // then lets its own event loop drain.
            'const w = new Worker("data:text/javascript,"); w.addEventListener("message", () => {});'
          );
          middle.addEventListener("message", () => {});
          await new Promise(r => middle.addEventListener("close", r, { once: true }));
        }
      `,
      ],
      env: bunEnv,
      stdout: "pipe",
      stderr: "pipe",
    });

    const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
    expect(stderr).toBe("");
    expect(stdout).toBe("");
    expect(exitCode).toBe(0);
  },
  timeout,
);

// Regression: issue #31224. A worker that schedules an async Bun.write to
// stdout and then `process.exit(0)`s in the same tick queues a write whose
// completion lands after the worker VM started shutting down. On Windows,
// the libuv loop pumps pending `uv_fs_write` completions during worker
// shutdown (`Loop::shutdown` walks handles and runs one `uv_run` to flush
// close callbacks). Before the fix, `WriteFileWindows::on_finish` entered
// the tearing-down worker's event loop and resolved `WriteFilePromise` on
// a dead JSC global, which drained microtasks via `JSNextTickQueue::drain`
// and segfaulted inside `JSC::Interpreter::executeCallImpl`. The related
// `FileSink` pipe-writer path (`PipeWriter::onWriteComplete` →
// `FileSink::onWrite` → `run_pending` → `event_loop.exit` →
// `drainMicrotasks`) crashed the same way.
//
// The fix discards unobservable completions once the worker VM is shutting
// down: `WriteFileWindows::on_finish` checks `VirtualMachine::is_shutting_down`
// and calls `WriteFilePromise::discard` + `deinit` without entering the
// event loop, and `FileSink::run_pending` calls `WritablePending::discard`
// so the stored `WritableFuture` is dropped without touching JSPromise
// resolution machinery. The worker must close cleanly (exit 0) and must
// not print the sentinel error that would indicate the `.then` ran
// against a shutting-down global.
//
// Windows-only: the crash path is `uv_fs_write` on the libuv-backed
// `WriteFileWindows`. POSIX uses a thread-pool `WorkTask` whose
// completion drops into the concurrent queue and is not dispatched past
// worker shutdown, so the hazard is not reachable there.
test.skipIf(!isWindows)(
  "worker that schedules Bun.write to stdout then exits does not crash",
  async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `
        const workers = [];
        for (let i = 0; i < ${perRound}; i++) {
          workers.push(new Worker("data:text/javascript," + encodeURIComponent(\`
            Bun.write(Bun.stdout, new Uint8Array(512 * 1024).fill(120)).then(() => {
              console.error("stdout write microtask ran after worker shutdown");
              process.exit(42);
            });
            process.exit(0);
          \`)));
        }
        const codes = await Promise.all(workers.map(w => new Promise(r => {
          // Workers started with a data: URL fire 'close' with an 'exitCode' field.
          w.addEventListener("close", e => r(e.code ?? e.exitCode ?? 0), { once: true });
        })));
        for (const c of codes) if (c !== 0) { console.error("bad exit", c); process.exit(1); }
      `,
      ],
      env: bunEnv,
      stdout: "pipe",
      stderr: "pipe",
    });

    const [stderr, exitCode] = await Promise.all([proc.stderr.text(), proc.exited]);
    expect(stderr).not.toContain("stdout write microtask ran after worker shutdown");
    expect(stderr).not.toContain("bad exit");
    expect(exitCode).toBe(0);
  },
  timeout,
);
