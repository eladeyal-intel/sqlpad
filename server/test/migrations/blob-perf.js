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

function csvStringify(obj) {
  return papa.unparse(obj);
}

function csvParse(buf) {
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
    // additional fields for larger set
    color: faker.commerce.color(),
    department: faker.commerce.department(),
    productName: faker.commerce.productName(),
    price: faker.commerce.price(),
    productAdjective: faker.commerce.productAdjective(),
    productMaterial: faker.commerce.productMaterial(),
    product: faker.commerce.product(),
    color2: faker.commerce.color(),
    department2: faker.commerce.department(),
    productName2: faker.commerce.productName(),
    price2: faker.commerce.price(),
    productAdjective2: faker.commerce.productAdjective(),
    productMaterial2: faker.commerce.productMaterial(),
    product2: faker.commerce.product(),
    date1: faker.date.past(),
    date2: faker.date.past(),
    date3: faker.date.past(),
    date4: faker.date.past(),
    date5: faker.date.past(),
    date6: faker.date.past(),
    date7: faker.date.past(),
    date8: faker.date.past(),
    date9: faker.date.past(),
    date10: faker.date.past(),
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
      for (let i = 0; i < 10; i++) {
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
      for (let i = 0; i < 10; i++) {
        const cache = await utils.sequelizeDb.Cache.findOne({
          where: { id: `id-${i}` },
        });
        const obj = cache.toJSON();
        if (decodefn) {
          obj[field] = await decodefn(obj[field]);
        }

        // if (i === 0) {
        //   console.log(obj[field][0]);
        // }
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

  describe.skip('data json', async function () {
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'data');
    testSelect('data');
  });

  describe.skip('blob json', async function () {
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'blob', JSON.stringify);
    testSelect('blob', JSON.parse);
  });

  describe.skip('blob json compressed', async function () {
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'blob', compressJson);
    testSelect('blob', decompressJson);
  });

  describe.skip('blob csv raw', async function () {
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'blob', csvStringify);
    testSelect('blob', csvParse);
  });

  describe.skip('blob csv zip', async function () {
    testInsert(FAKE_QUERY_RESULT_ARR_OF_ARR, 'blob', compressCsv);
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
