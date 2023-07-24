-- Databricks notebook source
select * 
from system.information_schema.tables
where table_name = 'results';