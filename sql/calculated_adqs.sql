-- return one row of data per presentation
WITH
  numbered AS (
  SELECT
    Practice_Code,
    BNF_Code,
    BNF_Description,
    Items,
    Quantity,
    ADQ_Usage,
    ROW_NUMBER() OVER (PARTITION BY BNF_Code ORDER BY BNF_Code) AS rownum
  FROM
    {}
  WHERE
    ADQ_Usage > 0 )
SELECT
  Practice_Code,
  BNF_Code,
  BNF_Description,
  Items,
  Quantity,
  ADQ_Usage
FROM
  numbered
WHERE
  rownum = 1
