/**
 * Smoke test: verifies loader + schema tool work with temp CSV data.
 * Run: bun run test-smoke.ts
 */
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { HealthDataDB } from './src/db/database';
import { FileCatalog } from './src/db/catalog';
import { TableLoader, buildTableSelectParts } from './src/db/loader';
import { HealthSchemaTool } from './src/tools/health-schema';

let tmpDir: string;
let db: HealthDataDB;

function assert(condition: boolean, msg: string) {
  if (!condition) throw new Error(`FAIL: ${msg}`);
  console.log(`  PASS: ${msg}`);
}

function createCSV(name: string, rows: string[]) {
  const content = `sep=,\r\n${rows.join('\r\n')}\r\n`;
  writeFileSync(join(tmpDir, name), content);
}

async function setup() {
  tmpDir = mkdtempSync(join(tmpdir(), 'health-test-'));

  // Quantity type (has unit column)
  const now = new Date();
  const recent = new Date(now.getTime() - 7 * 86400000);
  const ts = (d: Date) => d.toISOString().replace('T', ' ').slice(0, 19) + ' +0000';

  createCSV('HKQuantityTypeIdentifierHeartRate.csv', [
    'type,sourceName,sourceVersion,unit,startDate,endDate,value,device,productType',
    `HKQuantityTypeIdentifierHeartRate,Watch,10.0,count/min,${ts(recent)},${ts(recent)},72,Apple Watch,Watch7.1`,
    `HKQuantityTypeIdentifierHeartRate,Watch,10.0,count/min,${ts(now)},${ts(now)},80,Apple Watch,Watch7.1`,
  ]);

  // Category type (has unit column but value is string)
  createCSV('HKCategoryTypeIdentifierSleepAnalysis.csv', [
    'type,sourceName,sourceVersion,unit,startDate,endDate,value,device,productType',
    `HKCategoryTypeIdentifierSleepAnalysis,Watch,10.0,,${ts(recent)},${ts(recent)},AsleepCore,Apple Watch,Watch7.1`,
    `HKCategoryTypeIdentifierSleepAnalysis,Watch,10.0,,${ts(now)},${ts(now)},AsleepDeep,Apple Watch,Watch7.1`,
  ]);

  // Table without unit column (e.g. some workout files)
  createCSV('HKQuantityTypeIdentifierStepCount.csv', [
    'type,sourceName,sourceVersion,startDate,endDate,value,device,productType',
    `HKQuantityTypeIdentifierStepCount,iPhone,17.0,${ts(recent)},${ts(recent)},1234,iPhone,iPhone15.1`,
    `HKQuantityTypeIdentifierStepCount,iPhone,17.0,${ts(now)},${ts(now)},5678,iPhone,iPhone15.1`,
  ]);

  db = new HealthDataDB({ dataDir: tmpDir, maxMemoryMB: 256 });
  await db.initialize();
}

async function teardown() {
  await db.close();
  rmSync(tmpDir, { recursive: true, force: true });
}

async function testBuildTableSelectParts() {
  console.log('\n--- buildTableSelectParts ---');

  // Quantity with unit
  const p1 = buildTableSelectParts(['type', 'unit', 'value', 'startdate'], 'hkquantitytypeidentifierheartrate');
  assert(p1.unitSelect === 'unit,', 'unit present → select unit');
  assert(p1.valueSelect.includes('TRY_CAST'), 'quantity → TRY_CAST DOUBLE');
  assert(p1.tzSelect === '', 'no hktimezone → empty');

  // Category
  const p2 = buildTableSelectParts(['type', 'unit', 'value'], 'hkcategorytypeidentifiersleepanalysis');
  assert(p2.valueSelect.includes('VARCHAR'), 'category → CAST VARCHAR');

  // Missing unit
  const p3 = buildTableSelectParts(['type', 'value', 'startdate'], 'hkquantitytypeidentifierstepcount');
  assert(p3.unitSelect === 'NULL AS unit,', 'no unit → NULL AS unit');

  // Has HKTimeZone
  const p4 = buildTableSelectParts(['type', 'unit', 'value', 'hktimezone'], 'hkquantitytypeidentifierheartrate');
  assert(p4.tzSelect === ', HKTimeZone', 'hktimezone present → select it');
}

async function testLoaderQuantityType() {
  console.log('\n--- Loader: quantity type (HeartRate) ---');
  const catalog = new FileCatalog(tmpDir);
  await catalog.initialize();
  const loader = new TableLoader(db, catalog);

  await loader.ensureTableLoaded('hkquantitytypeidentifierheartrate');
  const entry = catalog.getEntry('hkquantitytypeidentifierheartrate');
  assert(entry!.loaded === true, 'marked as loaded');

  const rows = await db.execute('SELECT * FROM hkquantitytypeidentifierheartrate');
  assert(rows.length === 2, `got ${rows.length} rows`);
  assert(typeof rows[0].value === 'number', `value is number (${typeof rows[0].value})`);
  assert(rows[0].unit === 'count/min', `unit preserved: ${rows[0].unit}`);
}

async function testLoaderCategoryType() {
  console.log('\n--- Loader: category type (SleepAnalysis) ---');
  const catalog = new FileCatalog(tmpDir);
  await catalog.initialize();
  const loader = new TableLoader(db, catalog);

  await loader.ensureTableLoaded('hkcategorytypeidentifiersleepanalysis');

  const rows = await db.execute('SELECT * FROM hkcategorytypeidentifiersleepanalysis');
  assert(rows.length === 2, `got ${rows.length} rows`);
  assert(typeof rows[0].value === 'string', `value is string (${typeof rows[0].value})`);
  assert(rows[0].value === 'AsleepCore' || rows[0].value === 'AsleepDeep', `value content: ${rows[0].value}`);
}

async function testLoaderMissingUnit() {
  console.log('\n--- Loader: missing unit column (StepCount) ---');
  const catalog = new FileCatalog(tmpDir);
  await catalog.initialize();
  const loader = new TableLoader(db, catalog);

  await loader.ensureTableLoaded('hkquantitytypeidentifierstepcount');

  const rows = await db.execute('SELECT * FROM hkquantitytypeidentifierstepcount');
  assert(rows.length === 2, `got ${rows.length} rows`);
  assert(rows[0].unit === null, `unit is null when column missing: ${rows[0].unit}`);
  assert(typeof rows[0].value === 'number', `value is number: ${rows[0].value}`);
}

async function testSchemaToolPreview() {
  console.log('\n--- Schema tool: preview table isolation ---');
  // Use a fresh DB so no tables are pre-loaded
  const freshDb = new HealthDataDB({ dataDir: tmpDir, maxMemoryMB: 256 });
  await freshDb.initialize();
  const catalog = new FileCatalog(tmpDir);
  await catalog.initialize();
  const schemaTool = new HealthSchemaTool(freshDb, catalog);

  const result = await schemaTool.execute();

  // Schema tool should return table details for heartrate
  assert(result.tableDetails['hkquantitytypeidentifierheartrate'] !== undefined, 'heartrate in schema details');
  const hrDetail = result.tableDetails['hkquantitytypeidentifierheartrate'];
  assert(!hrDetail.error, `no error for heartrate: ${hrDetail.error || 'ok'}`);
  assert(hrDetail.columns.length > 0, `has columns: ${hrDetail.columns.length}`);

  // Preview table should NOT linger
  const tables = await freshDb.execute(
    `SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%_preview'`
  );
  assert(tables.length === 0, `no preview tables lingering (found ${tables.length})`);

  // Catalog should NOT be marked as loaded (schema only previews)
  const entry = catalog.getEntry('hkquantitytypeidentifierheartrate');
  assert(entry!.loaded === false, 'not marked as loaded after schema preview');

  // Full load should still work after schema preview
  const loader = new TableLoader(freshDb, catalog);
  await loader.ensureTableLoaded('hkquantitytypeidentifierheartrate');
  const entry2 = catalog.getEntry('hkquantitytypeidentifierheartrate');
  assert(entry2!.loaded === true, 'marked as loaded after full load');

  await freshDb.close();
}

async function main() {
  try {
    await setup();
    await testBuildTableSelectParts();
    await testLoaderQuantityType();
    await testLoaderCategoryType();
    await testLoaderMissingUnit();
    await testSchemaToolPreview();
    console.log('\n✅ All tests passed!\n');
  } catch (e) {
    console.error('\n❌ Test failed:', e);
    process.exit(1);
  } finally {
    await teardown();
  }
}

main();
