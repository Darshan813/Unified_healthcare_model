# Databricks notebook source
print('Bheruji')

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

storageAccountName = "darshanadls"
storageAccountAccessKey = dbutils.secrets.get('adls_scope_name_kv', 'adls-darshanadls-KV')
mountPoints=["gold","silver","bronze","landing-zone","configs"]
for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(mountPoint, storageAccountName),
            mount_point = f"/mnt/{mountPoint}",
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            )
            print(f"{mountPoint} mount succeeded!")
        except Exception as e:
            print("mount exception", e) 

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/audit.db/load_logs/_delta_log/

# COMMAND ----------


