
### Recover from corrupted db file:

``` sh
# 检查结构
sqlite3 corrupt.db "pragma integrity_check"
# 恢复数据
sqlite3 corrupt.db ".recover" | sqlite3 new.db
# 重新检查结构
sqlite3 new.db "pragma integrity_check"
```

Ref: [Python如何解决sqlite3.DatabaseError: database disk image is malformed_漫步量化-CSDN博客](https://blog.csdn.net/The_Time_Runner/article/details/106590571)
