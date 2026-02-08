from AssetTask import AssetTask
from EntityDaoSqlite import EntityDaoSqlite
import json

class AssetTaskDaoSqlite(EntityDaoSqlite):
    def __init__(self, dbConn):
        super().__init__(dbConn)
        self.tableName = "asset_task"
       
    async def InitDb(self):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
        await asyncDbConn.execute("DROP INDEX IF EXISTS index_asset_task_code")
        await asyncDbConn.execute("DROP INDEX IF EXISTS index_asset_task_id_client_id")
        await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
            (ID            INTEGER PRIMARY KEY AUTOINCREMENT,
            ASSET_ID       INTEGER NOT NULL,                                       
            VERSION        INTEGER NOT NULL,
            CLIENT_ID      INTEGER NOT NULL,
            MESSAGE_ID     TEXT,
            CODE           TEXT    NOT NULL,
            DESCRIPTION    TEXT    NOT NULL,         
            IS_RFS         BOOLEAN     NOT NULL);''')
        await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_asset_task_code 
                    ON {self.tableName}(code)''')
        await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_asset_task_id_client_id
                    ON {self.tableName}(id, client_id)''')

        await asyncDbConn.commit()

    async def AddAssetTask(self, assetTask):
        self.entityCount += 1
        lastId = 0        
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"INSERT INTO {self.tableName} (ASSET_ID, VERSION, CLIENT_ID, CODE, DESCRIPTION, IS_RFS, MESSAGE_ID) \
            VALUES ({assetTask['assetId']}, {assetTask['version']}, {assetTask['clientId']}, '{assetTask['code']}', '{assetTask['description']}', {assetTask['isRfs']}, '{assetTask['messageId']}')")

        await asyncDbConn.commit()

        async with asyncDbConn.execute(f"select last_insert_rowid();") as cursor:                
            async for row in cursor:        
                lastId = row[0]

        newAssetTask = await self.GetAssetTaskById(lastId)                                              
        return newAssetTask            

    async def UpdateAssetTask(self, id, assetTask):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                    SET \
                                    VERSION = VERSION + 1, \
                                    CODE = '{assetTask['code']}', \
                                    DESCRIPTION = '{assetTask['description']}', \
                                    IS_RFS = {assetTask['isRfs']}, \
                                    MESSAGE_ID = '{assetTask['messageId']}' \
                                    WHERE ID = '{id}'")

        await asyncDbConn.commit()    
        updatedAssetTask = await self.GetAssetTaskById(id)                     
        return updatedAssetTask                        

    async def GetAssetTaskById(self, id):
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f"SELECT id, asset_id, version, client_id, code, description, is_rfs, message_id FROM {self.tableName} WHERE Id = '{id}'") as cursor:                
            assetTask = None

            async for row in cursor:
                assetTask = {}
                assetTask["id"] = row[0]            
                assetTask["assetId"] = row[1]
                assetTask["version"] = row[2]                        
                assetTask["clientId"] = row[3]                                        
                assetTask["code"] = row[4]
                assetTask["description"] = row[5]
                assetTask["isRfs"] = bool(row[6])
                assetTask["messageId"] = row[7]

        return assetTask            

    async def GetAssetTaskByCode(self, clientId, code):
        asyncDbConn = self.dbConn                        

        async with asyncDbConn.execute(f"SELECT id, asset_id, version, client_id, code, description, is_rfs, message_id FROM {self.tableName} WHERE code= '{code}'") as cursor:                
            assetTask = None

            async for row in cursor:
                assetTask = {}
                assetTask["id"] = row[0]            
                assetTask["assetId"] = row[1]                                        
                assetTask["version"] = row[2]                        
                assetTask["clientId"] = row[3]
                assetTask["code"] = row[4]
                assetTask["description"] = row[5]
                assetTask["isRfs"] = bool(row[6])
                assetTask["messageId"] = row[7]

        return assetTask            

    async def GetAssetTaskCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteAssetTask(self, id):
        await super().DeleteEntity(id)

    async def GetTaskIdsForAsset(self, assetId):
        asyncDbConn = self.dbConn
        ids = []

        async with asyncDbConn.execute(f"SELECT ID FROM {self.tableName} WHERE asset_id = {assetId}") as cursor:
            async for row in cursor:
                ids.append(row[0])

        return ids

    async def DeleteAllAssetTasks(self):
        await super().DeleteAllEntities()        
