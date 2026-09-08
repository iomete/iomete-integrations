(function dsbuilder(attr) {
  var url =
    "jdbc:arrow-flight-sql://" +
    attr[connectionHelper.attributeServer] +
    ":" +
    attr[connectionHelper.attributePort] +
    "?cluster=" +
    encodeURIComponent(attr["v-compute-cluster"]) +
    "&data-plane=" +
    encodeURIComponent(attr["v-namespace"]) +
    "&useEncryption=true";

  if (attr["v-disable-cert-verification"] === "true") {
    url += "&disableCertificateVerification=true";
  }

  var connectTimeout = attr["v-connect-timeout"];
  var tunables = {
    queryTimeout: attr["v-query-timeout"],
    connectTimeoutMs: connectTimeout ? String(connectTimeout * 1000) : "",
    threadPoolSize: attr["v-thread-pool-size"],
  };

  Object.keys(tunables).forEach(function (name) {
    var value = tunables[name];

    if (value !== undefined && value !== "") {
      url += "&" + name + "=" + encodeURIComponent(value);
    }
  });

  var catalog = attr[connectionHelper.attributeDatabase];

  if (catalog !== undefined && catalog !== "") {
    url +=
      "&catalogFilterEnabled=true&schema=" + encodeURIComponent(catalog);
  }

  return [url];
})
