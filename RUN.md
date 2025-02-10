1. **Khởi động containers**
```bash
docker compose up -d
```

2. **Khởi tạo replica sets**
```bash
docker compose exec configsvr01 sh -c "mongosh < /scripts/init-configserver.js"
docker compose exec shard01-a sh -c "mongosh < /scripts/init-shard01.js"
docker compose exec shard02-a sh -c "mongosh < /scripts/init-shard02.js" 
docker compose exec shard03-a sh -c "mongosh < /scripts/init-shard03.js"
```

3. **Khởi tạo router**
```bash
docker compose exec router01 sh -c "mongosh < /scripts/init-router.js"
```

4. **Cấu hình write concern cho cluster**
```bash
docker compose exec router01 mongosh --eval 'db.adminCommand({setDefaultRWConcern: 1, defaultWriteConcern: {w: "majority", wtimeout: 5000}})'
```

5. **Cấu hình sharding**
```bash
docker compose exec router01 mongosh --port 27017

# Enable sharding cho database
sh.enableSharding("MyDatabase")

# Setup sharding key cho collection
db.adminCommand( { shardCollection: "MyDatabase.MyCollection", key: { oemNumber: "hashed", zipCode: 1, supplierId: 1 }, numInitialChunks: 3 } )
```

**Cấu trúc Cluster bao gồm:**
- 3 config servers (replica set)
- 3 shards (mỗi shard là 1 replica set với 3 nodes)
- 2 routers (mongos)

**Connection string để kết nối từ bên ngoài:**
```
mongodb://127.0.0.1:27117,127.0.0.1:27118
```

**Kiểm tra cấu hình:**
```bash
# Kiểm tra default RW concern
docker compose exec router01 mongosh --eval 'db.adminCommand({getDefaultRWConcern: 1})'

# Kiểm tra status của các shards
docker compose exec shard01-a mongosh --eval "rs.status()"
```

**Lưu ý quan trọng:** 
- Trên Windows/OSX cần lưu ý vấn đề tương thích với VirtualBox
- Các file config cần được lưu ở định dạng Unix (LF) thay vì Windows (CRLF)
- Cần đợi config server và shards bầu primary xong mới khởi tạo router
- Write concern được cấu hình ở mức cluster thay vì replica set
