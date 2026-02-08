import json
from ToDoItem import ToDoItem
from EntityDaoPg import EntityDaoPg

class ToDoDaoPgSql(EntityDaoPg):
    def __init__(self):
        self.tableName = "todo_item"        
        super().__init__(self.tableName)
       
    async def InitDb(self):
        await super().InitDb()        
        asyncDbConn = await self.GetDbConnection()
     
        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
            await asyncDbConn.execute(f"DROP INDEX IF EXISTS index_todo_item_name")            
            await asyncDbConn.execute(f"DROP INDEX IF EXISTS index_todo_item_id_client_id")                        

            await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
                    (ID             BIGSERIAL PRIMARY KEY,
                    VERSION        INTEGER NOT NULL,
                    CLIENT_ID      INTEGER NOT NULL,                               
                    MESSAGE_ID     TEXT,
                    NAME           TEXT    NOT NULL,
                    DESCRIPTION    TEXT    NOT NULL,         
                    IS_COMPLETE    BOOLEAN     NOT NULL);''')
            
            await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_todo_item_name 
                                ON {self.tableName}(name)''')
            await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_todo_item_id_client_id
                                ON {self.tableName}(id, client_id)''')
            
        await self.ReleaseDbConnection(asyncDbConn)
            
    async def AddItem(self, item):
        asyncDbConn = await self.GetDbConnection()
        lastId = 0

        async with asyncDbConn.transaction():        
            lastId = await asyncDbConn.fetchval(f"INSERT INTO {self.tableName} (VERSION, CLIENT_ID, MESSAGE_ID, NAME, DESCRIPTION, IS_COMPLETE) \
                            VALUES ({item['version']}, {item['clientId']}, '{item['messageId']}', '{item['name']}', '{item['description']}', {item['isComplete']}) RETURNING ID")
            
        await self.ReleaseDbConnection(asyncDbConn)

        newItem = await self.GetItemById(lastId)
        return newItem        

    async def UpdateItem(self, id, item):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():        
            await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                        SET \
                                        VERSION = VERSION + 1, \
                                        NAME = '{item['name']}', \
                                        DESCRIPTION = '{item['description']}', \
                                        IS_COMPLETE = {item['isComplete']}, \
                                        MESSAGE_ID = '{item['messageId']}' \
                                        WHERE ID = {id}")

        await self.ReleaseDbConnection(asyncDbConn)

        updatedItem = await self.GetItemById(id)                     
        return updatedItem  

    async def GetItemById(self, id):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            sql = f"SELECT id, version, client_id, name, description, is_complete, message_id FROM {self.tableName} WHERE ID = {id}"

            row = await asyncDbConn.fetchrow(sql)
            item = None

            if row:
                item = {}                
                item["id"] = row[0]            
                item["version"] = row[1]                        
                item["clientId"] = row[2]                                                            
                item["name"] = row[3]
                item["description"] = row[4]
                item["isComplete"] = bool(row[5])
                item["messageId"] = row[6]                

        await self.ReleaseDbConnection(asyncDbConn)
        return item            

    async def GetAllItems(self):
        result = []
        
        for key in self.todoDb:
            item = self.todoDb[key]            
            result.append(json.loads(item))
            
        return result

    async def GetItemCount(self):
        count = await super().GetEntityCount()        
        return count

    async def DeleteItem(self, id):
        await super().DeleteEntity(id)        

    async def DeleteAllItems(self):
        await super().DeleteAllEntities()                
