const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const connectorDirectory = path.join(__dirname, "..", "connector");
const connectionHelper = {
  attributeDatabase: "dbname",
  attributePassword: "password",
  attributePort: "port",
  attributeServer: "server",
  attributeUsername: "username",
};

function readConnectorFile(filename) {
  return fs.readFileSync(path.join(connectorDirectory, filename), "utf8");
}

function loadBuilder(filename) {
  return vm.runInNewContext(readConnectorFile(filename), { connectionHelper });
}

test("builds an encrypted, credential-free URL for all catalogs", () => {
  const buildConnection = loadBuilder("connectionBuilder.js");
  const attributes = {
    server: "release.iomete.cloud",
    port: "443",
    "v-compute-cluster": "dbt compute/primary",
    "v-namespace": "spark resources & 1",
    dbname: "",
    username: "admin",
    password: "secret-token",
  };

  const [url] = buildConnection(attributes);

  assert.equal(
    url,
    "jdbc:arrow-flight-sql://release.iomete.cloud:443?cluster=dbt%20compute%2Fprimary&data-plane=spark%20resources%20%26%201&useEncryption=true",
  );
});

test("disables certificate verification only when opted in", () => {
  const buildConnection = loadBuilder("connectionBuilder.js");
  const attributes = {
    server: "onprem.internal",
    port: "443",
    "v-compute-cluster": "dbt-compute",
    "v-namespace": "spark-resources-1",
  };

  const [optedOutUrl] = buildConnection({
    ...attributes,
    "v-disable-cert-verification": "false",
  });
  assert.doesNotMatch(optedOutUrl, /disableCertificateVerification/);

  const [optedInUrl] = buildConnection({
    ...attributes,
    "v-disable-cert-verification": "true",
  });
  assert.match(optedInUrl, /&useEncryption=true&disableCertificateVerification=true$/);
});

test("sends tunables only when they are supplied", () => {
  const buildConnection = loadBuilder("connectionBuilder.js");
  const attributes = {
    server: "release.iomete.cloud",
    port: "443",
    "v-compute-cluster": "dbt-compute",
    "v-namespace": "spark-resources-1",
  };

  const [defaultUrl] = buildConnection(attributes);
  assert.doesNotMatch(defaultUrl, /queryTimeout|connectTimeoutMs|threadPoolSize/);

  const [blankUrl] = buildConnection({
    ...attributes,
    "v-query-timeout": "",
    "v-connect-timeout": "",
    "v-thread-pool-size": "",
  });
  assert.doesNotMatch(blankUrl, /queryTimeout|connectTimeoutMs|threadPoolSize/);

  const [tunedUrl] = buildConnection({
    ...attributes,
    "v-query-timeout": "300",
    "v-connect-timeout": "30",
    "v-thread-pool-size": "4",
  });
  assert.match(tunedUrl, /&queryTimeout=300&connectTimeoutMs=30000&threadPoolSize=4$/);

  const [partialUrl] = buildConnection({ ...attributes, "v-thread-pool-size": "8" });
  assert.match(partialUrl, /&threadPoolSize=8$/);
  assert.doesNotMatch(partialUrl, /queryTimeout|connectTimeoutMs/);
});

test("adds an encoded catalog scope when a catalog is supplied", () => {
  const buildConnection = loadBuilder("connectionBuilder.js");

  const [url] = buildConnection({
    server: "release.iomete.cloud",
    port: "443",
    "v-compute-cluster": "dbt-compute",
    "v-namespace": "spark-resources-1",
    dbname: "finance/eu & shared",
  });

  assert.equal(
    url,
    "jdbc:arrow-flight-sql://release.iomete.cloud:443?cluster=dbt-compute&data-plane=spark-resources-1&useEncryption=true&catalogFilterEnabled=true&schema=finance%2Feu%20%26%20shared",
  );
});

test("passes credentials only as JDBC properties", () => {
  const buildProperties = loadBuilder("connectionProperties.js");

  const properties = buildProperties({
    username: "admin",
    password: "token=with-special/value",
  });

  assert.deepEqual(Object.keys(properties).sort(), ["password", "user"]);
  assert.equal(properties.user, "admin");
  assert.equal(properties.password, "token=with-special/value");
});

test("the server field rejects anything that is not a bare host", () => {
  const fields = readConnectorFile("connectionFields.xml");
  const [, pattern] = fields.match(
    /name="server"[\s\S]*?<validation-rule reg-exp="([^"]+)"/,
  ) || [];

  assert.ok(pattern, "connectionFields.xml declares no validation rule for server");

  const rule = new RegExp(pattern);

  for (const host of ["release.iomete.cloud", "iomete", "data-plane-1.eu.iomete.cloud"]) {
    assert.ok(rule.test(host), `${host} should be accepted`);
  }

  for (const invalid of [
    "https://release.iomete.cloud",
    "release.iomete.cloud/flight",
    "release.iomete.cloud:443",
    "release.iomete.cloud?x=1",
    "release .iomete.cloud",
    "",
  ]) {
    assert.ok(!rule.test(invalid), `${invalid} should be rejected`);
  }
});

test("every v- attribute read by the builder is declared in the connector XML", () => {
  const builder = readConnectorFile("connectionBuilder.js");
  const fields = readConnectorFile("connectionFields.xml");
  const resolver = readConnectorFile("connectionResolver.tdr");
  const used = new Set((builder.match(/"v-[a-z-]+"/g) || []).map((token) => token.slice(1, -1)));

  assert.ok(used.size > 0);

  for (const name of used) {
    assert.match(fields, new RegExp(`name="${name}"`), `${name} missing from connectionFields.xml`);
    assert.match(resolver, new RegExp(`<attr>${name}</attr>`), `${name} missing from connectionResolver.tdr`);
  }
});
