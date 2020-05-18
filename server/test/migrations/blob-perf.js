/* eslint-disable no-console */
/* eslint-disable no-await-in-loop */
const assert = require('assert');
const zlib = require('zlib');
const { promisify } = require('util');
const path = require('path');
const ncp = require('ncp').ncp;
const TestUtils = require('../utils');
const faker = require('faker');
const papa = require('papaparse');

const compress = promisify(zlib.deflate);
const decompress = promisify(zlib.unzip);

function compressCsv(obj) {
  return compress(papa.unparse(obj));
}

async function decompressCsv(compressedBuf) {
  const buf = await decompress(compressedBuf);
  const { data } = papa.parse(buf.toString());
  return data;
}

function compressJson(obj) {
  return compress(JSON.stringify(obj));
}

async function decompressJson(zippedBuf) {
  const buf = await decompress(zippedBuf);
  return JSON.parse(buf.toString());
}

ncp.limit = 16;

const sourceDir = path.join(__dirname, '../fixtures/v4.2.0-test-db/testdb');

const FAKE_QUERY_RESULT_ARR_OF_OBJ = [];
for (let i = 0; i < 10000; i++) {
  FAKE_QUERY_RESULT_ARR_OF_OBJ.push({
    name: faker.name.findName(),
    randomEmail: faker.internet.email(),
    streetName: faker.address.streetName(),
    streetAddress: faker.address.streetAddress(),
    streetSuffix: faker.address.streetSuffix(),
    streetPrefix: faker.address.streetPrefix(),
    county: faker.address.county(),
    country: faker.address.country(),
    countryCode: faker.address.countryCode(),
    state: faker.address.state(),
    stateAbbr: faker.address.stateAbbr(),
    latitude: faker.address.latitude(),
    longitude: faker.address.longitude(),
  });
}

const FAKE_QUERY_RESULT_ARR_OF_ARR = FAKE_QUERY_RESULT_ARR_OF_OBJ.map((row) =>
  Object.values(row)
);

function copyDbFiles(source, destination) {
  return new Promise((resolve, reject) => {
    ncp(source, destination, function (err) {
      if (err) {
        return reject(err);
      }
      return resolve();
    });
  });
}

describe('blob-perf', function () {
  this.timeout('10m');
  /**
   * @type {TestUtils}
   */
  let utils;

  function testInsert(data, field, encodefn) {
    it('Inserts', async function () {
      for (let i = 0; i < 100; i++) {
        if (i % 10 === 0) {
          console.log(`inserting ${i}`);
        }

        const cacheData = {
          id: `id-${i}`,
          name: `test data ${i}`,
          expiryDate: new Date(),
          [field]: encodefn ? await encodefn(data) : data,
        };

        await utils.sequelizeDb.Cache.create(cacheData);
      }
    });
  }

  function testSelect(field, decodefn) {
    it('selects', async function () {
      for (let i = 0; i < 100; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        const obj = cache.toJSON();
        if (decodefn) {
          obj[field] = await decodefn(obj[field]);
        }

        if (i === 0) {
          console.log(obj[field][0]);
        }
      }
    });
  }

  before('preps the env', async function () {
    utils = new TestUtils({
      dbPath: path.join(__dirname, '../artifacts/v4-to-v5'),
      dbInMemory: false,
    });

    const destination = utils.config.get('dbPath');

    await utils.prepDbDir();
    await copyDbFiles(sourceDir, destination);

    await utils.initDbs();
  });

  after(function () {
    return utils.sequelizeDb.sequelize.close();
  });

  it('zips as expected', async function () {
    const obj = {
      a: 1,
      b: true,
      c: '1234',
    };
    const zipped = await compressJson(obj);
    const unzipped = await decompressJson(zipped);
    assert.deepStrictEqual(unzipped, obj);
  });

  it('Migrates', async function () {
    await utils.migrate();
  });

  describe.skip('data json array of objects', async function () {
    // 313mb
    // 6168ms
    testInsert(FAKE_QUERY_RESULT_ARR_OF_OBJ, 'data');
    // 5707ms
    testSelect('data');
  });

  describe.skip('data json array of array', async function () {
    // 160mb
    // 3881ms
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'data');
    // 3524ms
    testSelect('data');
  });

  describe.skip('blob json array of obj', async function () {
    // 313mb
    // 3742ms
    testInsert(FAKE_QUERY_RESULT_ARR_OF_OBJ, 'blob', JSON.stringify);
    // 3058ms
    testSelect('blob', JSON.parse);
  });

  describe.skip('blob json array of array', async function () {
    // 160mb
    // 2606ms
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'blob', JSON.stringify);
    // 1778ms
    testSelect('blob', JSON.parse);
  });

  describe('blob compressed array of array', async function () {
    // 60mb
    // 10297ms
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'blob', compressJson);
    // 4028ms
    testSelect('blob', decompressJson);
  });

  describe.skip('blob csv zip array of array', async function () {
    // 55mb
    // 11246ms / 112ms each
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'blob', compressCsv);
    // 3401ms / 34ms each
    testSelect('blob', decompressCsv);
  });

  describe.skip('Stress test', async function () {
    it('Inserts, reads, deletes - blob zipped', async function () {
      for (let batch = 1; batch <= 20; batch++) {
        console.log(`batch ${batch}`);

        for (let i = 0; i < 50; i++) {
          const data = {
            id: `id-${i * batch}`,
            name: `test data ${i * batch}`,
            expiryDate: new Date(),
            blob: await compressJson(FAKE_QUERY_RESULT_ARR_OF_ARR),
          };

          await utils.sequelizeDb.Cache.create(data);
        }

        for (let i = 0; i < 50; i++) {
          const id = `id-${i * batch}`;
          const cache = await utils.sequelizeDb.Cache.findOne({
            where: { id },
          });
          const obj = cache.toJSON();
          obj.blob = await decompressJson(obj.blob);

          await utils.sequelizeDb.Cache.destroy({ where: { id } });
        }
      }
    });
  });
});
