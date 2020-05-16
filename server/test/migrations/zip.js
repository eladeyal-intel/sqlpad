const fs = require('fs');
const path = require('path');
const { deflate, unzip } = require('zlib');
const { promisify } = require('util');

const writeFileAzync = promisify(fs.writeFile);
const readFileAsync = promisify(fs.readFile);

const deflateAsync = promisify(deflate);
const unzipAsync = promisify(unzip);

exports.deflateJson = function deflateJson(obj) {
  return deflateAsync(JSON.stringify(obj));
};

exports.unzipJson = async function unzipJson(zippedBuf) {
  const buf = await unzipAsync(zippedBuf);
  return JSON.parse(buf.toString());
};

exports.writeZipped = async function writeZipped(id, data) {
  const filepath = path.join(
    __dirname,
    `../artifacts/v4-to-v5/cache/${id}.json`
  );
  const buff = await deflateAsync(JSON.stringify(data));
  return writeFileAzync(filepath, buff);
};

exports.readZipped = async function readZipped(id) {
  const filepath = path.join(
    __dirname,
    `../artifacts/v4-to-v5/cache/${id}.json`
  );
  const zippedBuf = await readFileAsync(filepath);
  const buf = await unzipAsync(zippedBuf);
  return JSON.parse(buf.toString());
};
