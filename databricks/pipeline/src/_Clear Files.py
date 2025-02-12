# Databricks notebook source
# Apagar tudo dentro de /FileStore
dbutils.fs.rm("dbfs:/tmp", recurse=True)

# Apagar tudo dentro de /FileStore
# Listar arquivos dentro de /FileStore
file_list = dbutils.fs.ls("dbfs:/FileStore")

# Apagar cada arquivo individualmente
for file in file_list:
    dbutils.fs.rm(file.path, recurse=True)


# Apagar tudo dentro de /mnt
# Listar arquivos dentro de /FileStore
file_list = dbutils.fs.ls("dbfs:/mnt/datalake")

# Apagar cada arquivo individualmente
for file in file_list:
    dbutils.fs.rm(file.path, recurse=True)
