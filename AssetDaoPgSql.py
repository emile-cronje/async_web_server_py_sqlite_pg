import json
from Asset import Asset
from EntityDaoPg import EntityDaoPg

class AssetDaoPgSql(EntityDaoPg):
    def __init__(self):
        self.tableName = "asset"
        super().__init__(self.tableName)        
       
    async def InitDb(self):
        await super().InitDb()        
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DROP TABLE IF EXISTS asset_task")                        
            await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
            await asyncDbConn.execute(f"DROP INDEX IF EXISTS index_asset_code")            
            await asyncDbConn.execute(f"DROP INDEX IF EXISTS index_asset_id_client_id")                        

            await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
                    (ID            SERIAL PRIMARY KEY,
                    VERSION        INTEGER NOT NULL,
                    CLIENT_ID      INTEGER NOT NULL,                               
                    MESSAGE_ID     TEXT,                                      
                    GUID           TEXT    NOT NULL,                    
                    CODE           TEXT    NOT NULL,
                    DESCRIPTION    TEXT    NOT NULL,         
                    IS_MSI    BOOLEAN     NOT NULL);''')
            
            await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_asset_code
                                ON {self.tableName}(code)''')
            await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_asset_id_client_id
                                ON {self.tableName}(id, client_id)''')
            
        await self.ReleaseDbConnection(asyncDbConn)

    async def AddAsset(self, asset):
        asyncDbConn = await self.GetDbConnection()
        lastId = 0

        async with asyncDbConn.transaction():        
            lastId = await asyncDbConn.fetchval(f"INSERT INTO {self.tableName} (VERSION, CLIENT_ID, GUID, MESSAGE_ID, CODE, DESCRIPTION, IS_MSI) \
                            VALUES ({asset['version']}, {asset['clientId']}, '{asset['guid']}', '{asset['messageId']}', '{asset['code']}', '{asset['description']}', {asset['isMsi']}) RETURNING ID")
            
        await self.ReleaseDbConnection(asyncDbConn)

        newAsset = await self.GetAssetById(lastId)
        return newAsset 

    async def UpdateAsset(self, id, asset):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():        
            await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                        SET \
                                        VERSION = VERSION + 1, \
                                        CODE = '{asset['code']}', \
                                        DESCRIPTION = '{asset['description']}', \
                                        IS_MSI = {asset['isMsi']}, \
                                        MESSAGE_ID = '{asset['messageId']}' \
                                        WHERE ID = {id}")

        await self.ReleaseDbConnection(asyncDbConn)

        updatedAsset = await self.GetAssetById(id)                     
        return updatedAsset

    async def GetAssetById(self, id):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            sql = f"SELECT id, version, client_id, code, description, is_msi, message_id, guid FROM {self.tableName} WHERE ID = {id}"

            row = await asyncDbConn.fetchrow(sql)
            asset = None

            if row:
                asset = {}                
                asset["id"] = row[0]            
                asset["version"] = row[1]                        
                asset["clientId"] = row[2]                                                            
                asset["code"] = row[3]
                asset["description"] = row[4]
                asset["isMsi"] = bool(row[5])
                asset["messageId"] = row[6]                
                asset["guid"] = row[7]                                

        await self.ReleaseDbConnection(asyncDbConn)
        return asset            

    async def GetAssetCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteAsset(self, id):
        await super().DeleteEntity(id)                

    async def DeleteAllAssets(self):
        await super().DeleteAllEntities()