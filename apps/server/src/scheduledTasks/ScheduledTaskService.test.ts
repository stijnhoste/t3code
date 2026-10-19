import * as Scheduler from "../scheduling/Scheduler.ts";
import * as NodeUtil from "node:util";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import { ProjectId, ScheduledTaskError, ScheduledTaskId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as SqlClient from "effect/sql/SqlClient";

import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import { DispatchModeLimit } from "../orchestration-v2/DispatchModeLimit.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ScheduledTaskService from "./ScheduledTaskService.ts";

const isScheduledTaskError = Schema.is(ScheduledTaskError);

const insertRow = (
  sql: SqlClient.SqlClient,
  row: {
    id: string | null;
    next: string | null;
    enabled: number;
    status: string;
    scheduleJson?: string;
    prompt?: string;
  },
  now: string,
) =>
  sql`INSERT INTO scheduled_tasks ${sql.insert({
    task_id: row.id,
    title: "task",
    prompt: row.prompt ?? "Run task",
    enabled: row.enabled,
    schedule_json: row.scheduleJson ?? '{"type":"interval","everyMs":60000}',
    project_id: "project:test",
    thread_id: null,
    workspace_strategy_json: '{"type":"root"}',
    model_selection_json: '{"instanceId":"codex","model":"gpt-5"}',
    runtime_mode: "full-access",
    interaction_mode: "default",
    created_by: "user",
    creation_source: "web",
    created_at: now,
    updated_at: now,
    next_run_at: row.next,
    last_run_at: null,
    last_run_status: row.status,
    last_run_error: null,
    run_count: 0,
  })}`;

it.effect("loads only due tasks and skips corrupt due rows without decoding settled tasks", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const now = "2026-09-09T12:00:00.000Z";
    const secret = "PROMPT-SECRET-DO-NOT-LOG";
    const credential = "CREDENTIAL-SECRET-DO-NOT-LOG";
    for (const row of [
      { id: "due-now", next: now, enabled: 1, status: "never" },
      { id: "", next: now, enabled: 1, status: "never" },
      { id: "   ", next: now, enabled: 1, status: "never" },
      { id: null, next: now, enabled: 1, status: "never" },
      {
        id: "due-earlier",
        next: "2026-09-09T11:00:00.000Z",
        enabled: 1,
        status: "failed",
      },
      { id: "due-bad-date", next: "", enabled: 1, status: "never" },
      {
        id: "due-corrupt",
        next: now,
        enabled: 1,
        status: "never",
        scheduleJson: "broken",
      },
      {
        id: "due-cred",
        next: now,
        enabled: 1,
        status: "never",
        scheduleJson: `{"type":"interval","everyMs":"${credential}"}`,
      },
      { id: "disabled", next: now, enabled: 0, status: "never", scheduleJson: "broken" },
      {
        id: "future",
        next: "2026-09-09T12:00:00.001Z",
        enabled: 1,
        status: "never",
        scheduleJson: "broken",
      },
      {
        id: "unscheduled",
        next: null,
        enabled: 1,
        status: "never",
        scheduleJson: "broken",
      },
      { id: "running", next: now, enabled: 1, status: "running", scheduleJson: "broken" },
    ]) {
      yield* insertRow(sql, { ...row, prompt: secret }, now);
    }
    const warnings: unknown[] = [];
    const tasks = yield* ScheduledTaskService.listDueTasks(DateTime.makeUnsafe(now)).pipe(
      Effect.provide(
        Logger.layer([
          Logger.make(({ message }) => {
            warnings.push(message);
          }),
        ]),
      ),
    );
    assert.deepEqual(
      tasks.map((task) => task.id),
      ["due-earlier", "due-now"],
    );
    assert.equal(warnings.length, 6);
    const annotations = warnings.map(
      (warning) => (warning as ReadonlyArray<unknown>)[1] as Record<string, unknown>,
    );
    assert.deepEqual(
      annotations.map((annotation) => annotation.taskId),
      ["due-bad-date", null, "", "   ", "due-corrupt", "due-cred"],
    );
    const causes = annotations.map((annotation) => annotation.cause);
    assert.deepEqual(
      causes.map((cause) => (isScheduledTaskError(cause) ? cause.taskId : undefined)),
      [undefined, undefined, undefined, undefined, "due-corrupt", "due-cred"],
    );
    for (const cause of causes) {
      if (cause !== undefined) assert.isTrue(isScheduledTaskError(cause));
    }
    const rendered = NodeUtil.inspect(annotations, { depth: null });
    assert.isFalse(rendered.includes(secret));
    assert.isFalse(rendered.includes(credential));
    const running = yield* sql<{
      last_run_status: string;
    }>`SELECT last_run_status FROM scheduled_tasks WHERE task_id = 'running'`;
    assert.equal(running[0]?.last_run_status, "running");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "releases interrupted runs on startup and still executes due tasks, skipping corrupt rows",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-09-09T12:00:00.000Z";
      const hugeInterval = '{"type":"interval","everyMs":9000000000000000}';
      for (const row of [
        { id: "stuck-valid", next: now, status: "running" },
        { id: "stuck-corrupt", next: now, status: "running", scheduleJson: "broken" },
        { id: "stuck-huge", next: now, status: "running", scheduleJson: hugeInterval },
        { id: null, next: now, status: "running" },
        { id: "due-bad-date", next: "", status: "never" },
        { id: "due-healthy", next: now, status: "never" },
        { id: "due-huge", next: now, status: "never", scheduleJson: hugeInterval },
        { id: "due-second", next: now, status: "never" },
        { id: "due-third", next: now, status: "never" },
      ]) {
        yield* insertRow(sql, { ...row, enabled: 1 }, now);
      }

      // it.effect freezes the clock; pin it just past the seeded due time so
      // the poll sees the rows as due.
      yield* TestClock.setTime(Date.parse(now) + 1_000);
      const dispatched = yield* Ref.make(0);
      const lastDispatched = yield* Deferred.make<void>();
      const releaseLast = yield* Deferred.make<void>();
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Layer.build(
            Layer.provideMerge(
              ScheduledTaskService.layer,
              Layer.mergeAll(
                Layer.mock(ThreadLaunchService.ThreadLaunchService)({
                  launch: () =>
                    Ref.updateAndGet(dispatched, (n) => n + 1).pipe(
                      Effect.andThen((n) =>
                        n === 4
                          ? Deferred.succeed(lastDispatched, undefined).pipe(
                              Effect.andThen(Deferred.await(releaseLast)),
                            )
                          : Effect.void,
                      ),
                      Effect.andThen(Effect.die(new Error("test launch failure"))),
                    ),
                }),
                Layer.mock(ThreadManagementService.ThreadManagementService)({}),
                Layer.mock(SecretRequests.SecretRequests)({}),
                NodeCrypto.layer,
                Scheduler.layer,
              ),
            ),
          );
          // The poll loop delays before its first pass, so the frozen test
          // clock must advance once to trigger it.
          yield* TestClock.adjust("6 seconds");
          yield* Deferred.await(lastDispatched);
          const inflight = yield* sql<{
            last_run_status: string;
          }>`SELECT last_run_status FROM scheduled_tasks WHERE task_id = 'due-third'`;
          assert.equal(inflight[0]?.last_run_status, "running");
          yield* Deferred.succeed(releaseLast, undefined);
        }),
      );

      const rows = yield* sql<{
        task_id: string | null;
        last_run_status: string;
        last_run_error: string | null;
        next_run_at: string | null;
        run_count: number;
      }>`SELECT task_id, last_run_status, last_run_error, next_run_at, run_count
       FROM scheduled_tasks ORDER BY task_id`;
      const byId = new Map(rows.map((row) => [row.task_id, row]));
      assert.equal(rows.length, 9);
      assert.equal(byId.get("stuck-valid")?.last_run_status, "failed");
      assert.isNotNull(byId.get("stuck-valid")?.next_run_at);
      assert.equal(byId.get("stuck-corrupt")?.last_run_status, "failed");
      assert.equal(byId.get(null)?.last_run_status, "failed");
      const stuckHuge = byId.get("stuck-huge");
      assert.equal(stuckHuge?.last_run_status, "failed");
      assert.isNull(stuckHuge?.next_run_at);
      for (const id of ["stuck-valid", "stuck-corrupt", "stuck-huge", null]) {
        const row = byId.get(id);
        assert.equal(row?.last_run_error, "Run was interrupted by a server restart.");
        assert.equal(row?.run_count, 1);
      }
      for (const id of ["due-healthy", "due-huge", "due-second"]) {
        const row = byId.get(id);
        assert.equal(row?.last_run_status, "failed");
        assert.isTrue(row?.last_run_error?.includes("test launch failure") === true);
        assert.equal(row?.run_count, 1);
      }
      assert.isNull(byId.get("due-huge")?.next_run_at);
      assert.equal(byId.get("due-bad-date")?.last_run_status, "never");
      assert.equal(byId.get("due-bad-date")?.run_count, 0);
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "treats a next_run_at corrupted before the dispatch re-read as not due instead of defecting the poll",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-09-09T12:00:00.000Z";
      for (const row of [
        { id: "run-a", next: now, status: "never" },
        { id: "run-b-victim", next: now, status: "never" },
        { id: "run-c", next: now, status: "never" },
        { id: "run-d", next: now, status: "never" },
      ]) {
        yield* insertRow(sql, { ...row, enabled: 1 }, now);
      }
      yield* TestClock.setTime(Date.parse(now) + 1_000);
      const dispatched = yield* Ref.make(0);
      const lastDispatched = yield* Deferred.make<void>();
      const releaseLast = yield* Deferred.make<void>();
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Layer.build(
            Layer.provideMerge(
              ScheduledTaskService.layer,
              Layer.mergeAll(
                Layer.mock(ThreadLaunchService.ThreadLaunchService)({
                  launch: () =>
                    Ref.updateAndGet(dispatched, (n) => n + 1).pipe(
                      Effect.andThen((n) =>
                        n === 1
                          ? sql`UPDATE scheduled_tasks
                                SET next_run_at = ''
                                WHERE task_id = 'run-b-victim'`.pipe(Effect.asVoid, Effect.orDie)
                          : n === 3
                            ? Deferred.succeed(lastDispatched, undefined).pipe(
                                Effect.andThen(Deferred.await(releaseLast)),
                              )
                            : Effect.void,
                      ),
                      Effect.andThen(Effect.die(new Error("test launch failure"))),
                    ),
                }),
                Layer.mock(ThreadManagementService.ThreadManagementService)({}),
                Layer.mock(SecretRequests.SecretRequests)({}),
                NodeCrypto.layer,
                Scheduler.layer,
              ),
            ),
          );
          yield* TestClock.adjust("6 seconds");
          yield* Deferred.await(lastDispatched);
          assert.equal(yield* Ref.get(dispatched), 3);
          const rows = yield* sql<{
            task_id: string;
            last_run_status: string;
            next_run_at: string | null;
            run_count: number;
          }>`SELECT task_id, last_run_status, next_run_at, run_count
             FROM scheduled_tasks ORDER BY task_id`;
          const byId = new Map(rows.map((row) => [row.task_id, row]));
          const victim = byId.get("run-b-victim");
          assert.equal(victim?.last_run_status, "never");
          assert.equal(victim?.run_count, 0);
          assert.equal(victim?.next_run_at, "");
          for (const id of ["run-a", "run-c"]) {
            const row = byId.get(id);
            assert.equal(row?.last_run_status, "failed");
            assert.equal(row?.run_count, 1);
          }
          yield* Deferred.succeed(releaseLast, undefined);
        }),
      );
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

const atomicTaskId = ScheduledTaskId.make("atomic-task");
const atomicProjectId = ProjectId.make("project:test");
const atomicScope = { id: atomicTaskId, projectId: atomicProjectId };
const atomicDependencies = Layer.mergeAll(
  NodeCrypto.layer,
  Scheduler.layer,
  Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
  Layer.mock(ThreadManagementService.ThreadManagementService)({}),
  Layer.mock(SecretRequests.SecretRequests)({}),
);
const atomicLayer = ScheduledTaskService.layer.pipe(
  Layer.provide(atomicDependencies),
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const seedAtomicTask = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* insertRow(
    sql,
    {
      id: atomicTaskId,
      next: "2026-09-09T12:01:00.000Z",
      enabled: 1,
      status: "never",
    },
    "2026-09-09T12:00:00.000Z",
  );
  return (yield* (yield* ScheduledTaskService.ScheduledTaskService).list()).tasks[0]!;
});

it.effect("partial updates preserve concurrent fields and the pending run", () =>
  Effect.gen(function* () {
    const service = yield* ScheduledTaskService.ScheduledTaskService;
    const before = yield* seedAtomicTask;
    const results = yield* Effect.all(
      [
        service.update({ ...atomicScope, title: "Renamed" }),
        service.update({ ...atomicScope, prompt: "Changed elsewhere", runtimeMode: "auto" }),
      ],
      { concurrency: "unbounded" },
    );
    assert.isTrue(results.every(Option.isSome));
    const { tasks } = yield* service.list();
    assert.deepEqual(tasks, [
      {
        ...before,
        title: "Renamed",
        prompt: "Changed elsewhere",
        runtimeMode: "auto",
        updatedAt: tasks[0]!.updatedAt,
      },
    ]);
  }).pipe(Effect.provide(atomicLayer)),
);

it.effect("concurrent schedule, pause and sparse workspace edits preserve each other", () =>
  Effect.gen(function* () {
    const service = yield* ScheduledTaskService.ScheduledTaskService;
    const before = yield* seedAtomicTask;
    yield* service.upsert({
      ...before,
      workspaceStrategy: {
        type: "worktree",
        baseRef: "main",
        startFromOrigin: true,
        branch: "keep",
      },
    });
    yield* Effect.all(
      [
        service.update({
          ...atomicScope,
          schedule: { type: "interval", everyMs: 3_600_000 },
          workspaceStrategyPatch: { baseRef: "develop" },
        }),
        service.update({
          ...atomicScope,
          enabled: false,
          workspaceStrategyPatch: { startFromOrigin: false },
        }),
      ],
      { concurrency: "unbounded" },
    );
    const {
      tasks: [after],
    } = yield* service.list();
    assert.equal(after?.enabled, false);
    assert.isNull(after?.nextRunAt);
    assert.deepEqual(after?.schedule, { type: "interval", everyMs: 3_600_000 });
    assert.deepEqual(after?.workspaceStrategy, {
      type: "worktree",
      baseRef: "develop",
      startFromOrigin: false,
      branch: "keep",
    });
  }).pipe(Effect.provide(atomicLayer)),
);

it.effect("a deleted task stays deleted after concurrent and stale edits", () =>
  Effect.gen(function* () {
    const service = yield* ScheduledTaskService.ScheduledTaskService;
    yield* seedAtomicTask;
    yield* Effect.all(
      [service.update({ ...atomicScope, title: "Racing edit" }), service.delete(atomicScope)],
      { concurrency: "unbounded" },
    );
    assert.deepEqual((yield* service.list()).tasks, []);
    assert.isTrue(Option.isNone(yield* service.update({ ...atomicScope, title: "Stale edit" })));
    assert.deepEqual((yield* service.list()).tasks, []);
  }).pipe(Effect.provide(atomicLayer)),
);

it.effect("the old project cannot edit, delete or run a moved task", () =>
  Effect.gen(function* () {
    const service = yield* ScheduledTaskService.ScheduledTaskService;
    yield* seedAtomicTask;
    const moved = Option.getOrThrow(
      yield* service.update({
        ...atomicScope,
        nextProjectId: ProjectId.make("project:destination"),
      }),
    ).task;
    assert.isTrue(Option.isNone(yield* service.update({ ...atomicScope, title: "Stale edit" })));
    assert.isTrue(Option.isNone(yield* service.delete(atomicScope)));
    // The launch dependency is deliberately unimplemented: dispatch would defect.
    const rejected = yield* service.runNow(atomicScope).pipe(Effect.flip);
    assert.isTrue(isScheduledTaskError(rejected));
    assert.deepEqual((yield* service.list()).tasks, [moved]);
  }).pipe(Effect.provide(atomicLayer)),
);

it.effect("guarded mutations reject changed bindings and current or new bound-thread modes", () =>
  Effect.gen(function* () {
    const service = yield* ScheduledTaskService.ScheduledTaskService;
    const sql = yield* SqlClient.SqlClient;
    yield* seedAtomicTask;
    for (const id of ["bound-a", "bound-b"]) {
      yield* sql`
        INSERT INTO orchestration_v2_projection_threads (
          thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
          created_at, updated_at, payload_json
        ) VALUES (
          ${id}, ${atomicProjectId}, 'Thread', 'codex', 'approval-required', 'default',
          '2026-09-09T12:00:00.000Z', '2026-09-09T12:00:00.000Z', '{}'
        )
      `;
    }
    const first = ThreadId.make("bound-a");
    const second = ThreadId.make("bound-b");
    yield* service.update({ ...atomicScope, runtimeMode: "approval-required", threadId: first });
    yield* service.update({ ...atomicScope, threadId: second });
    const guard = {
      ...atomicScope,
      expectedThreadId: first,
      expectedRuntimeMode: "approval-required" as const,
    };
    assert.isTrue(
      isScheduledTaskError(yield* service.update({ ...guard, prompt: "Stale" }).pipe(Effect.flip)),
    );
    assert.isTrue(isScheduledTaskError(yield* service.delete(guard).pipe(Effect.flip)));
    yield* sql`UPDATE orchestration_v2_projection_threads SET runtime_mode = 'full-access' WHERE thread_id = ${second}`;
    const unchangedBinding = { ...guard, expectedThreadId: second };
    const limit = { runtimeMode: "approval-required", interactionMode: "default" } as const;
    assert.isTrue(
      isScheduledTaskError(
        yield* service
          .update({ ...unchangedBinding, prompt: "Stale" })
          .pipe(Effect.provideService(DispatchModeLimit, limit), Effect.flip),
      ),
    );
    assert.isTrue(
      isScheduledTaskError(
        yield* service
          .delete(unchangedBinding)
          .pipe(Effect.provideService(DispatchModeLimit, limit), Effect.flip),
      ),
    );
    assert.equal((yield* service.list()).tasks[0]?.prompt, "Run task");
    yield* sql`UPDATE orchestration_v2_projection_threads SET archived_at = '2026-09-09T12:00:00.000Z' WHERE thread_id = ${second}`;
    const edited = Option.getOrThrow(
      yield* service.update({ ...atomicScope, title: "Archived binding kept" }),
    ).task;
    assert.equal(edited.threadId, second);
    assert.equal(edited.title, "Archived binding kept");
    const unbound = Option.getOrThrow(
      yield* service.update({ ...atomicScope, threadId: null }),
    ).task;
    yield* sql`UPDATE orchestration_v2_projection_threads SET runtime_mode = 'full-access' WHERE thread_id = ${first}`;
    assert.isTrue(
      isScheduledTaskError(
        yield* service
          .update({ ...guard, expectedThreadId: null, threadId: first, title: "Stale rebind" })
          .pipe(Effect.provideService(DispatchModeLimit, limit), Effect.flip),
      ),
    );
    assert.deepEqual((yield* service.list()).tasks, [unbound]);
  }).pipe(Effect.provide(atomicLayer)),
);

it.effect("manual reservation rejects a project move after the initial load", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* insertRow(
      sql,
      { id: atomicTaskId, next: "2026-09-09T12:01:00.000Z", enabled: 1, status: "never" },
      "2026-09-09T12:00:00.000Z",
    );
    let moveBeforeReservation = false;
    const serviceSql = new Proxy(sql, {
      get(target, key) {
        if (key !== "withTransaction") return Reflect.get(target, key);
        return <A, E, R>(body: Effect.Effect<A, E, R>) =>
          Effect.suspend(() => {
            if (!moveBeforeReservation) return target.withTransaction(body);
            moveBeforeReservation = false;
            return sql`UPDATE scheduled_tasks SET project_id = 'project:destination' WHERE task_id = ${atomicTaskId}`.pipe(
              Effect.andThen(target.withTransaction(body)),
            );
          });
      },
    });
    const context = yield* Layer.build(
      ScheduledTaskService.layer.pipe(
        Layer.provide(atomicDependencies),
        Layer.provide(Layer.succeed(SqlClient.SqlClient, serviceSql)),
      ),
    );
    const service = yield* ScheduledTaskService.ScheduledTaskService.pipe(
      Effect.provideContext(context),
    );
    moveBeforeReservation = true;
    assert.isTrue(isScheduledTaskError(yield* service.runNow(atomicScope).pipe(Effect.flip)));
    const task = (yield* service.list()).tasks[0]!;
    assert.equal(task.projectId, "project:destination");
    assert.equal(task.lastRunStatus, "never");
    assert.equal(task.runCount, 0);
  }).pipe(Effect.scoped, Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("completion preserves a pause committed before its transaction", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* insertRow(
      sql,
      { id: atomicTaskId, next: "2026-09-09T12:01:00.000Z", enabled: 1, status: "never" },
      "2026-09-09T12:00:00.000Z",
    );
    let pauseBeforeCompletion = false;
    const serviceSql = new Proxy(sql, {
      get(target, key) {
        if (key !== "withTransaction") return Reflect.get(target, key);
        return <A, E, R>(body: Effect.Effect<A, E, R>) =>
          Effect.suspend(() => {
            if (!pauseBeforeCompletion) return target.withTransaction(body);
            pauseBeforeCompletion = false;
            return sql`UPDATE scheduled_tasks SET enabled = 0, next_run_at = NULL WHERE task_id = ${atomicTaskId}`.pipe(
              Effect.andThen(target.withTransaction(body)),
            );
          });
      },
    });
    const context = yield* Layer.build(
      ScheduledTaskService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            atomicDependencies,
            Layer.mock(ThreadLaunchService.ThreadLaunchService)({
              launch: () =>
                Effect.sync(() => {
                  pauseBeforeCompletion = true;
                }).pipe(Effect.andThen(Effect.die("test launch failure"))),
            }),
          ),
        ),
        Layer.provide(Layer.succeed(SqlClient.SqlClient, serviceSql)),
      ),
    );
    const service = yield* ScheduledTaskService.ScheduledTaskService.pipe(
      Effect.provideContext(context),
    );
    const result = yield* service.runNow(atomicScope);
    const task = (yield* service.list()).tasks[0]!;
    assert.equal(task.enabled, false);
    assert.isNull(task.nextRunAt);
    assert.equal(task.lastRunStatus, "failed");
    assert.equal(task.runCount, 1);
    assert.deepEqual(result.task, task);
  }).pipe(Effect.scoped, Effect.provide(SqlitePersistence.layerMemory)),
);
