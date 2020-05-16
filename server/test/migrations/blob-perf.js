/* eslint-disable no-await-in-loop */
const assert = require('assert');
const path = require('path');
const ncp = require('ncp').ncp;
const TestUtils = require('../utils');
const faker = require('faker');
const papa = require('papaparse');
const {
  deflateJson,
  unzipJson,
  readZipped,
  writeZipped,
  deflate,
  unzip,
} = require('./zip');

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
    const zipped = await deflateJson(obj);
    const unzipped = await unzipJson(zipped);
    assert.deepStrictEqual(unzipped, obj);
  });

  it('Migrates', async function () {
    await utils.migrate();
  });

  describe.skip('json data array of objects', async function () {
    // At 100 caches
    // 313mb
    // 6200 ms / 62 each
    it('Inserts - json data array of objects', async function () {
      for (let i = 0; i < 100; i++) {
        if (i % 10 === 0) {
          console.log(`inserting ${i}`);
        }
        await utils.sequelizeDb.Cache.create({
          id: `id-${i}`,
          name: `test data ${i}`,
          expiryDate: new Date(),
          data: FAKE_QUERY_RESULT_ARR_OF_OBJ,
        });
      }
    });

    // 5000ms / 50 each
    it('selects them', async function () {
      for (let i = 0; i < 100; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        cache.toJSON();
      }
    });
  });

  describe.skip('json data array of array', async function () {
    // At 100 caches
    // 161mb
    // 4300 ms / 43 each
    it('Inserts - json data array of array', async function () {
      for (let i = 0; i < 100; i++) {
        if (i % 10 === 0) {
          console.log(`inserting ${i}`);
        }
        await utils.sequelizeDb.Cache.create({
          id: `id-${i}`,
          name: `test data ${i}`,
          expiryDate: new Date(),
          data: FAKE_QUERY_RESULT_ARR_OF_ARR,
        });
      }
    });

    // 3800ms / 38 each
    it('selects them', async function () {
      for (let i = 0; i < 100; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        cache.toJSON();
      }
    });
  });

  describe.skip('blob data array of obj', async function () {
    // At 100 caches
    // 313mb
    // 4300 ms
    it('Inserts - blob', async function () {
      for (let i = 0; i < 100; i++) {
        if (i % 10 === 0) {
          console.log(`inserting ${i}`);
        }
        await utils.sequelizeDb.Cache.create({
          id: `id-${i}`,
          name: `test data ${i}`,
          expiryDate: new Date(),
          blob: JSON.stringify(FAKE_QUERY_RESULT_ARR_OF_OBJ),
        });
      }
    });

    // 3000
    it('selects them', async function () {
      for (let i = 0; i < 100; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        const obj = cache.toJSON();
        obj.blob = JSON.parse(obj.blob);
      }
    });
  });

  describe.skip('blob data array of array', async function () {
    // At 100 caches
    // 160mb
    // 2600 ms / 26ms each
    it('Inserts - blob', async function () {
      for (let i = 0; i < 100; i++) {
        if (i % 10 === 0) {
          console.log(`inserting ${i}`);
        }
        await utils.sequelizeDb.Cache.create({
          id: `id-${i}`,
          name: `test data ${i}`,
          expiryDate: new Date(),
          blob: JSON.stringify(FAKE_QUERY_RESULT_ARR_OF_ARR),
        });
      }
    });

    // 1900ms / 19ms each
    it('selects them', async function () {
      for (let i = 0; i < 100; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        const obj = cache.toJSON();
        obj.blob = JSON.parse(obj.blob);
      }
    });
  });

  describe.skip('blob data zip array of array', async function () {
    // At 100 caches
    // 60 mb
    // 10606ms / 106ms each
    it('Inserts - blob zipped', async function () {
      for (let i = 0; i < 100; i++) {
        if (i % 10 === 0) {
          console.log(`inserting ${i}`);
        }

        const data = {
          id: `id-${i}`,
          name: `test data ${i}`,
          expiryDate: new Date(),
          blob: await deflateJson(FAKE_QUERY_RESULT_ARR_OF_ARR),
        };

        await utils.sequelizeDb.Cache.create(data);
      }
    });

    // 2975ms / 30ms each
    it('selects them', async function () {
      for (let i = 0; i < 100; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        const obj = cache.toJSON();
        obj.blob = await unzipJson(obj.blob);
      }
    });
  });

  describe('blob csv zip array of array', async function () {
    // At 100 caches
    // 55 mb
    // 11900
    it('Inserts - blob zipped', async function () {
      for (let i = 0; i < 100; i++) {
        if (i % 10 === 0) {
          console.log(`inserting ${i}`);
        }

        const data = {
          id: `id-${i}`,
          name: `test data ${i}`,
          expiryDate: new Date(),
          blob: await deflate(papa.unparse(FAKE_QUERY_RESULT_ARR_OF_ARR)),
        };

        await utils.sequelizeDb.Cache.create(data);
      }
    });

    // 3501
    it('selects them', async function () {
      for (let i = 0; i < 100; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        const obj = cache.toJSON();
        const buff = await unzip(obj.blob);
        const data = await papa.parse(buff.toString());
        obj.data = data.data;

        if (i === 0) {
          console.log(obj.data[0]);
        }
      }
    });
  });

  describe.skip('file data zip array of array', async function () {
    // At 100 caches
    // 60mb
    // 10133ms / 101ms each
    it('Inserts - file zipped', async function () {
      for (let i = 0; i < 100; i++) {
        if (i % 10 === 0) {
          console.log(`inserting ${i}`);
        }

        const data = {
          id: `id-${i}`,
          name: `test data ${i}`,
          expiryDate: new Date(),
        };

        await utils.sequelizeDb.Cache.create(data);
        await writeZipped(data.id, FAKE_QUERY_RESULT_ARR_OF_ARR);
      }
    });

    // 2940 ms / 30ms each
    it('selects them', async function () {
      for (let i = 0; i < 100; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        const obj = cache.toJSON();
        obj.blob = await readZipped(obj.id);
      }
    });
  });

  describe.skip('Stress blob data zip array of array', async function () {
    // At 1000 caches
    // ?? mb

    it('Inserts, reads, deletes - blob zipped', async function () {
      for (let batch = 1; batch <= 20; batch++) {
        console.log(`batch ${batch}`);

        for (let i = 0; i < 50; i++) {
          const data = {
            id: `id-${i * batch}`,
            name: `test data ${i * batch}`,
            expiryDate: new Date(),
            blob: await deflateJson(FAKE_QUERY_RESULT_ARR_OF_ARR),
          };

          await utils.sequelizeDb.Cache.create(data);
        }

        for (let i = 0; i < 50; i++) {
          const id = `id-${i * batch}`;
          const cache = await utils.sequelizeDb.Cache.findOne({
            where: { id },
          });
          const obj = cache.toJSON();
          obj.blob = await unzipJson(obj.blob);

          await utils.sequelizeDb.Cache.destroy({ where: { id } });
        }
      }
    });
  });
});
