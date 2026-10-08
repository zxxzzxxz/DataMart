-- V1: 数据质量隔离区 schema（plugins/data_quality.py 的 quarantine 落点）
-- 违规行由 DQ 引擎同事务「复制到此 schema 下的隔离表 + 原表删除」，保留排查痕迹。
CREATE SCHEMA IF NOT EXISTS dwh_quarantine;
