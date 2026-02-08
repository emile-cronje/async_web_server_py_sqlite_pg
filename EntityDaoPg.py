import asyncpg

class EntityDaoPg:
    def __init__(self, tableName):
        self.dbConnPool = None
        self.tableName = tableName

    async def InitDb(self):
        self.dbConnPool = await asyncpg.create_pool(database="pg_crud_test_server",
                                                    user="postgres",
                                                    password="1793", host="localhost",
                                                    max_size = 100)                

    async def GetDbConnection(self):        
        return await self.dbConnPool.acquire()        

    async def ReleaseDbConnection(self, dbConn):        
        await self.dbConnPool.release(dbConn)                    
 #       print("after: current size: " + str(self.dbConnPool.get_idle_size()))                

    async def GetEntityCount(self):
        asyncDbConn = await self.GetDbConnection()
        count = 0        

        async with asyncDbConn.transaction():        
            count = await asyncDbConn.fetchval(f"SELECT COUNT(*) FROM {self.tableName}")

        await self.ReleaseDbConnection(asyncDbConn)

        return count
    
    async def DeleteEntity(self, id):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DELETE FROM {self.tableName} WHERE Id = {id}")                    

        await self.ReleaseDbConnection(asyncDbConn)

    async def DeleteAllEntities(self):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DELETE FROM {self.tableName}")                    

        await self.ReleaseDbConnection(asyncDbConn)



