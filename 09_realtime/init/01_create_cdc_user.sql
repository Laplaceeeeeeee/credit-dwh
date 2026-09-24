-- ============================================================
-- 为实时链路创建**最小权限**的 CDC 复制账号。
--
-- 为什么不直接用 root：
--   ① 最小权限原则 —— CDC 只需要读 binlog 与元数据，不该有写权限；
--   ② 实测踩坑：root 用的是 MySQL 8 默认的 caching_sha2_password，
--      在非 SSL 连接下驱动需要向服务器索取 RSA 公钥，而默认
--      allowPublicKeyRetrieval=false，于是报
--         java.sql.SQLNonTransientConnectionException:
--         Public Key Retrieval is not allowed
--      改用 mysql_native_password 的账号后，认证过程不需要公钥交换，
--      问题从根上消失（而不是靠放宽驱动参数绕过去）。
--
-- ⚠️ 另一条被证伪的思路（留档，避免重复走）：
--   给 CDC 源表加 'jdbc.properties.allowPublicKeyRetrieval' = 'true'
--   **无效**。因为报错来自 MySqlValidator，它用 MySqlSourceConfig
--   的 dbzProperties（Debezium 属性）建连，而不是 jdbc.properties
--   那张表（后者只用于 JdbcUrlUtils 拼快照/binlog 的连接串）。
--
-- 权限清单来自 Debezium 对 MySQL 的官方要求，一个不多一个不少。
-- ============================================================

CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED WITH mysql_native_password BY 'cdc_pwd_2026';

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'cdc'@'%';

FLUSH PRIVILEGES;

-- 自检：应看到 cdc 的 plugin = mysql_native_password
SELECT user, host, plugin FROM mysql.user WHERE user = 'cdc';
SHOW GRANTS FOR 'cdc'@'%';
