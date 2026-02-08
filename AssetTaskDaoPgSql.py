import json
from AssetTask import AssetTask
from EntityDaoPg import EntityDaoPg

class AssetTaskDaoPgSql(EntityDaoPg):
    def __init__(self, assetDao):
        self.tableName = "asset_task"
        super().__init__(self.tableName)                
        self.assetDao = assetDao
       
    async def InitDb(self):
        await super().InitDb()        
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
            await asyncDbConn.execute(f"DROP INDEX IF EXISTS index_asset_task_code")            
            await asyncDbConn.execute(f"DROP INDEX IF EXISTS index_asset_task_id_client_id")                        

            await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
                    (ID            SERIAL PRIMARY KEY,
                    ASSET_ID       INTEGER NOT NULL,                                                                      
                    VERSION        INTEGER NOT NULL,
                    CLIENT_ID      INTEGER NOT NULL,                                                              
                    MESSAGE_ID     TEXT,                                                                            
                    CODE           TEXT    NOT NULL,
                    DESCRIPTION    TEXT    NOT NULL,         
                    IS_RFS         BOOL     NOT NULL);''')            
            
            await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_asset_task_code
                                ON {self.tableName}(code)''')
            await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_asset_task_id_client_id
                                ON {self.tableName}(id, client_id)''')
            
        await self.ReleaseDbConnection(asyncDbConn)

    async def AddAssetTask(self, assetTask):
        asset = await self.assetDao.GetAssetById(assetTask["assetId"])

        if (asset == None):
            return None

        asyncDbConn = await self.GetDbConnection()

        lastId = 0        
        async with asyncDbConn.transaction():        
            lastId = await asyncDbConn.fetchval(f"INSERT INTO {self.tableName} (ASSET_ID, VERSION, CLIENT_ID, CODE, DESCRIPTION, IS_RFS, MESSAGE_ID) \
            VALUES ({assetTask['assetId']}, {assetTask['version']}, {assetTask['clientId']}, '{assetTask['code']}', '{assetTask['description']}', {assetTask['isRfs']}, '{assetTask['messageId']}') RETURNING ID")

        await self.ReleaseDbConnection(asyncDbConn)

        newAssetTask = await self.GetAssetTaskById(lastId)
        return newAssetTask 

    async def UpdateAssetTask(self, id, assetTask):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():        
            await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                    SET \
                                    VERSION = VERSION + 1, \
                                    CODE = '{assetTask['code']}', \
                                    DESCRIPTION = '{assetTask['description']}', \
                                    IS_RFS = {assetTask['isRfs']}, \
                                    MESSAGE_ID = '{assetTask['messageId']}' \
                                    WHERE ID = {id}")

        await self.ReleaseDbConnection(asyncDbConn)
        updatedAsset = await self.GetAssetTaskById(id)                             

        return updatedAsset

    async def GetAssetTaskById(self, id):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            sql = f"SELECT id, asset_id, version, client_id, code, description, is_rfs, message_id FROM {self.tableName} WHERE ID = {id}"

            row = await asyncDbConn.fetchrow(sql)
            assetTask = None

            if row:
                assetTask = {}                
                assetTask["id"] = row[0]            
                assetTask["assetId"] = row[1]                            
                assetTask["version"] = row[2]                        
                assetTask["clientId"] = row[3]                                                            
                assetTask["code"] = row[4]
                assetTask["description"] = row[5]
                assetTask["isRfs"] = bool(row[6])
                assetTask["messageId"] = row[7]                

        await self.ReleaseDbConnection(asyncDbConn)
        return assetTask            

    async def GetAssetTaskCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteAssetTask(self, id):
        await super().DeleteEntity(id)                        

    async def DeleteAssetTasksForAsset(self, assetId):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DELETE FROM {self.tableName} WHERE asset_id = {assetId}")                    

        await self.ReleaseDbConnection(asyncDbConn)

    async def GetTaskIdsForAsset(self, assetId):
        asyncDbConn = await self.GetDbConnection()

        rows = await asyncDbConn.fetch(f"SELECT ID FROM {self.tableName} WHERE asset_id = {assetId}")
        ids = [row['id'] for row in rows]        

        await self.ReleaseDbConnection(asyncDbConn)

        return ids

    async def DeleteAllAssetTasks(self):
        await super().DeleteAllEntities()        