class EntityDaoSqlite:
    def __init__(self, dbConn):
        self.dbConn = dbConn
        self.tableName = None
        self.entityCount = 0        

    async def GetEntityCount(self):
        count = 0        
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f"SELECT COUNT(*) FROM {self.tableName}") as cursor:                
            async for row in cursor:        
                count = row[0]

        return count

    async def DeleteEntity(self, id):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"DELETE FROM {self.tableName} WHERE Id = {id}")                    
        await asyncDbConn.commit()    

    async def DeleteAllEntities(self):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"DELETE FROM {self.tableName}")                    
        await asyncDbConn.commit()    
        self.entityCount = 0                

    def GetDbConnection(self):        
        return self.dbConn 
