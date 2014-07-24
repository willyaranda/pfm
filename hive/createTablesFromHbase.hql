create external table user (key string, username string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,username:username")
TBLPROPERTIES ("hbase.table.name" = "User");

create external table tweet (key string, text string, userId string, isRetweet string, userName string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,text:text,userId:userId,isRetweet:isRetweet,userName:userName")
TBLPROPERTIES ("hbase.table.name" = "Tweet");