/* eslint-disable no-await-in-loop */
const assert = require('assert');
const path = require('path');
const ncp = require('ncp').ncp;
const TestUtils = require('../utils');
const faker = require('faker');

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

  it('Migrates', async function () {
    await utils.migrate();
  });

  describe.skip('json data array of objects', async function () {
    // At 100 caches
    // 313mb
    // 6200 ms
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

    // 5000ms
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
    // 4300 ms
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

    // 3800ms
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

  describe('blob data array of array', async function () {
    // At 100 caches
    // 160mb
    // 2600 ms
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

    // 1900ms
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
});
