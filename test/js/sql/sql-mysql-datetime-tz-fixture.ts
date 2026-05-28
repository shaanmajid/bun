// A JS Date bound to a MySQL DATETIME and read back must be the same instant
// regardless of the process timezone. Encode breaks the Date's epoch-ms into
// Y/M/D h:m:s via pure-UTC arithmetic, so decode has to treat those components
// as UTC too — if it interprets them as local time, the round-trip silently
// shifts by the machine's UTC offset.
//
// The driving test spawns this fixture under several TZ values against a real
// MySQL server and asserts the identity holds for each.

import { SQL, randomUUIDv7 } from "bun";

const tls = process.env.CA_PATH ? { ca: Bun.file(process.env.CA_PATH) } : undefined;
await using sql = new SQL({
  url: process.env.MYSQL_URL,
  tls,
  max: 1,
  allowPublicKeyRetrieval: true,
});

const t = "dt_tz_" + randomUUIDv7("hex").replaceAll("-", "");
await sql`CREATE TEMPORARY TABLE ${sql(t)} (id INT PRIMARY KEY, dt DATETIME)`;
// Signal a live connection so the driving test can tell "no MySQL here"
// (soft-skip in local/sandboxed runs) apart from an actual decode failure.
console.log("CONNECTED");

const inputs = [
  new Date("2024-06-15T12:00:00.000Z"), // summer (DST active in zones that observe it)
  new Date("2024-01-15T00:30:00.000Z"), // winter, near midnight UTC — local-time misread crosses the day boundary
  new Date("2024-12-31T23:45:00.000Z"), // year boundary
];

for (let i = 0; i < inputs.length; i++) {
  await sql`INSERT INTO ${sql(t)} (id, dt) VALUES (${i}, ${inputs[i]})`;
}
const rows = await sql`SELECT id, dt FROM ${sql(t)} ORDER BY id`;

const failures: string[] = [];
for (let i = 0; i < inputs.length; i++) {
  const got: Date = rows[i].dt;
  if (!(got instanceof Date)) {
    failures.push(`id=${i}: expected Date, got ${Object.prototype.toString.call(got)}`);
    continue;
  }
  const want = inputs[i].getTime();
  const have = got.getTime();
  if (want !== have) {
    const diffMin = (have - want) / 60000;
    failures.push(`id=${i}: in=${inputs[i].toISOString()} out=${got.toISOString()} diffMin=${diffMin}`);
  }
}

if (failures.length) {
  console.error(`FAIL TZ=${process.env.TZ} offsetMin=${new Date().getTimezoneOffset()}`);
  for (const f of failures) console.error("  " + f);
  process.exit(1);
}

console.log(`OK TZ=${process.env.TZ} offsetMin=${new Date().getTimezoneOffset()}`);
