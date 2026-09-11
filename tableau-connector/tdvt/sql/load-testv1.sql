-- Recreates the TDVT TestV1 tables. Run only in a non-production test schema.
CREATE SCHEMA IF NOT EXISTS `__IOMETE_CATALOG__`.`__IOMETE_SCHEMA__`;

DROP TABLE IF EXISTS `__IOMETE_CATALOG__`.`__IOMETE_SCHEMA__`.`calcs`;

CREATE OR REPLACE TEMPORARY VIEW `_tdvt_calcs_csv` (
  `key` STRING,
  `num0` DOUBLE,
  `num1` DOUBLE,
  `num2` DOUBLE,
  `num3` DOUBLE,
  `num4` DOUBLE,
  `str0` STRING,
  `str1` STRING,
  `str2` STRING,
  `str3` STRING,
  `int0` INT,
  `int1` INT,
  `int2` INT,
  `int3` INT,
  `bool0` INT,
  `bool1` INT,
  `bool2` INT,
  `bool3` INT,
  `date0` DATE,
  `date1` DATE,
  `date2` DATE,
  `date3` DATE,
  `time0` TIMESTAMP,
  `time1` STRING,
  `datetime0` TIMESTAMP,
  `datetime1` STRING,
  `zzz` STRING
)
USING csv
OPTIONS (
  path '__IOMETE_TESTV1_URI__/Calcs_headers.csv',
  header 'true',
  mode 'FAILFAST',
  quote '"',
  escape '"',
  nullValue '',
  emptyValue '',
  dateFormat 'yyyy-MM-dd',
  timestampFormat 'yyyy-MM-dd HH:mm:ss'
);

CREATE TABLE `__IOMETE_CATALOG__`.`__IOMETE_SCHEMA__`.`calcs`
USING iceberg
AS SELECT
  `key`,
  `num0`,
  `num1`,
  `num2`,
  `num3`,
  `num4`,
  `str0`,
  `str1`,
  `str2`,
  `str3`,
  `int0`,
  `int1`,
  `int2`,
  `int3`,
  CASE `bool0` WHEN 0 THEN FALSE WHEN 1 THEN TRUE END AS `bool0`,
  CASE `bool1` WHEN 0 THEN FALSE WHEN 1 THEN TRUE END AS `bool1`,
  CASE `bool2` WHEN 0 THEN FALSE WHEN 1 THEN TRUE END AS `bool2`,
  CASE `bool3` WHEN 0 THEN FALSE WHEN 1 THEN TRUE END AS `bool3`,
  `date0`,
  `date1`,
  `date2`,
  `date3`,
  `time0`,
  `time1`,
  `datetime0`,
  `datetime1`,
  `zzz`
FROM `_tdvt_calcs_csv`;

DROP VIEW `_tdvt_calcs_csv`;

DROP TABLE IF EXISTS `__IOMETE_CATALOG__`.`__IOMETE_SCHEMA__`.`staples`;

CREATE OR REPLACE TEMPORARY VIEW `_tdvt_staples_csv` (
  `Item Count` INT,
  `Ship Priority` STRING,
  `Order Priority` STRING,
  `Order Status` STRING,
  `Order Quantity` DOUBLE,
  `Sales Total` DOUBLE,
  `Discount` DOUBLE,
  `Tax Rate` DOUBLE,
  `Ship Mode` STRING,
  `Fill Time` DOUBLE,
  `Gross Profit` DOUBLE,
  `Price` DECIMAL(18,4),
  `Ship Handle Cost` DECIMAL(18,4),
  `Employee Name` STRING,
  `Employee Dept` STRING,
  `Manager Name` STRING,
  `Employee Yrs Exp` DOUBLE,
  `Employee Salary` DECIMAL(18,4),
  `Customer Name` STRING,
  `Customer State` STRING,
  `Call Center Region` STRING,
  `Customer Balance` DOUBLE,
  `Customer Segment` STRING,
  `Prod Type1` STRING,
  `Prod Type2` STRING,
  `Prod Type3` STRING,
  `Prod Type4` STRING,
  `Product Name` STRING,
  `Product Container` STRING,
  `Ship Promo` STRING,
  `Supplier Name` STRING,
  `Supplier Balance` DOUBLE,
  `Supplier Region` STRING,
  `Supplier State` STRING,
  `Order ID` STRING,
  `Order Year` INT,
  `Order Month` INT,
  `Order Day` INT,
  `Order Date` TIMESTAMP,
  `Order Quarter` STRING,
  `Product Base Margin` DOUBLE,
  `Product ID` STRING,
  `Receive Time` DOUBLE,
  `Received Date` TIMESTAMP,
  `Ship Date` TIMESTAMP,
  `Ship Charge` DECIMAL(18,4),
  `Total Cycle Time` DOUBLE,
  `Product In Stock` STRING,
  `PID` INT,
  `Market Segment` STRING
)
USING csv
OPTIONS (
  path '__IOMETE_TESTV1_URI__/Staples_utf8_headers.csv',
  header 'true',
  mode 'FAILFAST',
  quote '"',
  escape '"',
  nullValue '',
  emptyValue '',
  dateFormat 'yyyy-MM-dd',
  timestampFormat 'yyyy-MM-dd HH:mm:ss'
);

CREATE TABLE `__IOMETE_CATALOG__`.`__IOMETE_SCHEMA__`.`staples`
USING iceberg
AS SELECT *
FROM `_tdvt_staples_csv`;

DROP VIEW `_tdvt_staples_csv`;
