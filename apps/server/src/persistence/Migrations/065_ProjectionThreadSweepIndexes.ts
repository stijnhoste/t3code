import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Match the sweep expressions so filtering reads index values instead of parsing PR snapshots.
  yield* sql`
    CREATE INDEX orchestration_v2_projection_threads_active_idx
    ON orchestration_v2_projection_threads(
      updated_at,
      thread_id,
      json_extract(payload_json, '$.settledAt'),
      json_extract(payload_json, '$.settledOverride'),
      json_extract(payload_json, '$.pinnedAt'),
      json_extract(payload_json, '$.autoSettleDisabledAt'),
      json_array_length(payload_json, '$.pullRequests')
    )
    WHERE deleted_at IS NULL AND archived_at IS NULL
  `;
});
