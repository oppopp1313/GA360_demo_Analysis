--New Visitor & Returning Visitor SQL code
SELECT
  -- User Type (dimension)
  CASE
    WHEN totals.newVisits = 1 THEN 'New Visitor'
  ELSE
  'Returning Visitor'
END
  AS User_Type,
  -- Count of Sessions (dimension)
  visitNumber AS Count_of_Sessions,
  -- Users (metric)
  COUNT(DISTINCT fullVisitorId) AS Users,
  -- New Users (metric)
  COUNT(DISTINCT(
      CASE
        WHEN totals.newVisits = 1 THEN fullVisitorId
      ELSE
      NULL
    END
      )) AS New_Users,
  -- % New Sessions (metric)
  COUNT(DISTINCT(
      CASE
        WHEN totals.newVisits = 1 THEN fullVisitorId
      ELSE
      NULL
    END
      )) / COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) AS Percentage_New_Sessions,
  -- Number of Sessions per User (metric)
  COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) / COUNT(DISTINCT fullVisitorId) AS Number_of_Sessions_per_User,
  -- Hits (metric)
  SUM((
    SELECT
      totals.hits
    FROM
      UNNEST(hits)
    GROUP BY
      1)) AS Hits
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _table_suffix BETWEEN '20160801'
  AND '20170801'
GROUP BY
  1,
  2
ORDER BY
  2



--Traffic SQL code
SELECT
  -- Referral Path (dimension)
  trafficSource.referralPath AS Referral_Path,
  -- Full Referrer (dimension)
  CONCAT(trafficSource.source,trafficSource.referralPath) AS Full_Referrer,
  -- Default Channel Grouping
  channelGrouping AS Default_Channel_Grouping,
  -- Campaign (dimension)
  trafficSource.campaign AS Campaign,
  -- Source (dimension)
  trafficSource.source AS Source,
  -- Medium (dimension)
  trafficSource.medium AS Medium,
  -- Source / Medium (dimension)
  CONCAT(trafficSource.source," / ",trafficSource.medium) AS Source_Medium,
  -- Keyword (dimension)
  trafficSource.keyword AS Keyword,
  -- Ad Content (dimension)
  trafficSource.adContent AS Ad_Content,
  -- Social Network (dimension)
  MAX(hits.social.socialNetwork) AS Social_Network,
  -- Social Source (dimension)
  MAX(hits.social.hasSocialSourceReferral) AS Social_Source,
  -- Campaign Code (dimension)
  trafficSource.campaignCode AS Campaign_Code,
  COUNT(DISTINCT fullVisitorId) AS Users
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits
WHERE
  _table_suffix BETWEEN '20160801'
  AND '20170801'
  AND totals.visits = 1
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  12

--Page & Pageviews SQL code
  SELECT
  Hostname,
  Page,
  Previous_Page,
  Page_Path_Level_1,
  Page_Path_Level_2,
  Page_Path_Level_3,
  Page_Path_Level_4,
  Page_Title,
  Landing_Page,
  Second_Page,
  Exit_Page,
  -- Entrances (metric)
  SUM(CASE
      WHEN isEntrance = TRUE THEN 1
    ELSE
    0
  END
    ) AS Entrances,
  -- Pageviews (metric)
  COUNT(*) AS Pageviews,
  -- Pages Per Session (metric)
  COUNT(*) / COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) AS Pages_Per_Session,
  -- Exits (metric)
  SUM(CASE
      WHEN isExit = TRUE THEN 1
    ELSE
    0
  END
    ) AS Exits,
  -- Exit Rate (metric)
  SUM(CASE
      WHEN isExit = TRUE THEN 1
    ELSE
    0
  END
    ) / COUNT(*) AS Exit_Rate
FROM (
  SELECT
    -- Hostname (dimension)
    hits.page.hostname AS Hostname,
    -- Page (dimension)
    hits.page.pagePath AS Page,
    -- Previous Page (dimension)
    LAG(hits.page.pagePath, 1) OVER (PARTITION BY fullvisitorId, visitStartTime ORDER BY hits.hitNumber ASC) AS Previous_Page,
    -- Page path level 1 (dimension)
    hits.page.pagePathLevel1 AS Page_Path_Level_1,
    -- Page path level 2 (dimension)
    hits.page.pagePathLevel2 AS Page_Path_Level_2,
    -- Page path level 3 (dimension)
    hits.page.pagePathLevel3 AS Page_Path_Level_3,
    -- Page path level 4 (dimension)
    hits.page.pagePathLevel4 AS Page_Path_Level_4,
    -- Page Title (dimension)
    hits.page.pageTitle AS Page_Title,
    -- Landing Page (dimension)
    CASE
      WHEN hits.isEntrance = TRUE THEN hits.page.pagePath
    ELSE
    NULL
  END
    AS Landing_page,
    -- Second Page (dimension)
    CASE
      WHEN hits.isEntrance = TRUE THEN (LEAD(hits.page.pagePath, 1) OVER (PARTITION BY fullvisitorId, visitStartTime ORDER BY hits.hitNumber ASC))
    ELSE
    NULL
  END
    AS Second_Page,
    -- Exit Page (dimension)
    CASE
      WHEN hits.isExit = TRUE THEN hits.page.pagePath
    ELSE
    NULL
  END
    AS Exit_page,
    hits.isEntrance,
    fullVisitorId,
    visitStartTime,
    hits.isExit
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    _table_suffix BETWEEN '20160801'
    AND '20170801'
    AND totals.visits = 1
    AND hits.type = 'PAGE')
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11
ORDER BY
  13 DESC



-- Geo SQL code
SELECT
  -- Continent (dimension)
  geoNetwork.continent AS Continent,
  -- Sub Continent (dimension)
  geoNetwork.subContinent AS Sub_Continent,
  -- Country (dimension)
  geoNetwork.country AS Country,
  -- Region (dimension)
  geoNetwork.region AS Region,
  -- Metro (dimension)
  geoNetwork.metro AS Metro,
  -- City (dimension)
  geoNetwork.city AS City,
   COUNT(DISTINCT fullVisitorId) AS Users
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _table_suffix BETWEEN '20160801'
  AND '20170801'
  AND totals.visits = 1
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6


-- Device SQL code
SELECT
  -- Browser (dimension)
  device.browser AS Browser,
  -- Operating System (dimension)
  device.operatingSystem AS Operating_System,
  -- Operating System Version (dimension),
  device.deviceCategory AS Device_Category,
  COUNT(DISTINCT fullVisitorId) AS Users
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits
WHERE
  _table_suffix BETWEEN '20160801'
  AND '20170801'
  AND totals.visits = 1
GROUP BY
  1,
  2,
  3