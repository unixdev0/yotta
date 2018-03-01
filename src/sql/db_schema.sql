CREATE TABLE IF NOT EXISTS schema_version (version INT);
DELETE FROM schema_version;
INSERT INTO schema_version VALUES(1);

--
-- instruments to subscribe to market data
--
CREATE TABLE IF NOT EXISTS instruments
(
    feedcode   CHAR(32) PRIMARY KEY
);

--
-- Initial instruments to be subscribed
--
DELETE FROM instruments;
INSERT INTO instruments VALUES('MSFT');
INSERT INTO instruments VALUES('YHOO');
INSERT INTO instruments VALUES('AAPL');
INSERT INTO instruments VALUES('TSLA');
INSERT INTO instruments VALUES('NVDA');
