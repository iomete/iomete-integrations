(function propertiesbuilder(attr) {
  var properties = {};
  properties["user"] = attr[connectionHelper.attributeUsername];
  properties["password"] = attr[connectionHelper.attributePassword];

  return properties;
})
