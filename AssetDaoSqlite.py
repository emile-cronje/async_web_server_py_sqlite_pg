from Asset import Asset
from EntityDaoSqlite import EntityDaoSqlite
import json

class AssetDaoSqlite(EntityDaoSqlite):
    def __init__(self, dbConn):
        super().__init__(dbConn)
        self.tableName = "asset"
       
    async def InitDb(self):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute("DROP TABLE IF EXISTS asset_task")
        await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
        await asyncDbConn.execute("DROP INDEX IF EXISTS index_asset_code")
        await asyncDbConn.execute("DROP INDEX IF EXISTS index_asset_id_client_id")
        await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
            (ID            INTEGER PRIMARY KEY AUTOINCREMENT,
            VERSION        INTEGER NOT NULL,
            CLIENT_ID      INTEGER NOT NULL,
            MESSAGE_ID     TEXT,
            GUID           TEXT    NOT NULL,
            CODE           TEXT    NOT NULL,
            DESCRIPTION    TEXT    NOT NULL,         
            IS_MSI         BOOLEAN     NOT NULL);''')
        await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_asset_code 
                    ON {self.tableName}(code)''')
        await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_asset_id_client_id
                    ON {self.tableName}(id, client_id)''')

        await asyncDbConn.commit()

    async def AddAsset(self, asset):
        self.entityCount += 1
        lastId = 0        
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"INSERT INTO {self.tableName} (VERSION, CLIENT_ID, GUID, MESSAGE_ID, CODE, DESCRIPTION, IS_MSI) \
            VALUES ({asset['version']}, {asset['clientId']}, '{asset['guid']}', '{asset['messageId']}', '{asset['code']}', '{asset['description']}', {asset['isMsi']})")

        await asyncDbConn.commit()

        async with asyncDbConn.execute(f"select last_insert_rowid();") as cursor:                
            async for row in cursor:        
                lastId = row[0]

        newAsset = await self.GetAssetById(lastId)                                              
        return newAsset            

    async def UpdateAsset(self, id, asset):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                    SET \
                                    VERSION = VERSION + 1, \
                                    CODE = '{asset['code']}', \
                                    DESCRIPTION = '{asset['description']}', \
                                    IS_MSI = {asset['isMsi']}, \
                                    MESSAGE_ID = '{asset['messageId']}' \
                                    WHERE ID = {id}")

        await asyncDbConn.commit()    
        updatedAsset = await self.GetAssetById(id)                     
        return updatedAsset                        

    async def GetAssetById(self, id):
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f"SELECT id, version, client_id, code, description, is_msi, message_id, guid FROM {self.tableName} WHERE Id = '{id}'") as cursor:                
            asset = None

            async for row in cursor:
                asset = {}
                asset["id"] = row[0]            
                asset["version"] = row[1]                        
                asset["clientId"] = row[2]                                                        
                asset["code"] = row[3]
                asset["description"] = row[4]
                asset["isMsi"] = bool(row[5])
                asset["messageId"] = row[6]
                asset["guid"] = row[7]

        return asset            

    async def GetAssetByCode(self, code):
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f"SELECT id, version, client_id, code, description, is_msi, message_id, guid FROM {self.tableName} WHERE code= '{code}'") as cursor:                
            asset = None

            async for row in cursor:
                asset = {}
                asset["id"] = row[0]            
                asset["version"] = row[1]                        
                asset["clientId"] = row[2]
                asset["code"] = row[3]
                asset["description"] = row[4]
                asset["isMsi"] = bool(row[5])
                asset["messageId"] = row[6]
                asset["guid"] = row[7]

        return asset            

    async def GetAssetCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteAsset(self, id):
        await super().DeleteEntity(id)

    async def DeleteAllAssets(self):
        await super().DeleteAllEntities()        
