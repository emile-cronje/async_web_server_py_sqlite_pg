from ToDoItem import ToDoItem
from EntityDaoSqlite import EntityDaoSqlite
import json

class ToDoDaoSqlite(EntityDaoSqlite):
    def __init__(self, dbConn):
        super().__init__(dbConn)
        self.tableName = "todo_item"

    async def InitDb(self):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
        await asyncDbConn.execute("DROP INDEX IF EXISTS index_todo_item_name")
        await asyncDbConn.execute("DROP INDEX IF EXISTS index_todo_item_id_client_id")
        await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
            (ID            INTEGER PRIMARY KEY AUTOINCREMENT,
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

        await asyncDbConn.commit()

    async def AddItem(self, item):
        self.entityCount += 1
        lastId = 0
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"INSERT INTO {self.tableName} (VERSION, CLIENT_ID, MESSAGE_ID, NAME, DESCRIPTION, IS_COMPLETE) \
            VALUES ({item['version']}, {item['clientId']}, '{item['messageId']}', '{item['name']}', '{item['description']}', {item['isComplete']})")

        await asyncDbConn.commit()

        async with asyncDbConn.execute(f"select last_insert_rowid();") as cursor:                
            async for row in cursor:        
                lastId = row[0]

        newItem = await self.GetItemById(lastId)
        return newItem        

    async def UpdateItem(self, id, item):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                    SET \
                                    VERSION = VERSION + 1, \
                                    NAME = '{item['name']}', \
                                    DESCRIPTION = '{item['description']}', \
                                    IS_COMPLETE = {item['isComplete']}, \
                                    MESSAGE_ID = '{item['messageId']}' \
                                    WHERE ID = {id}")

        await asyncDbConn.commit()    
        updatedItem = await self.GetItemById(id)                     
        return updatedItem                        

    async def GetItemById(self, id):
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f"SELECT id, version, client_id, name, description, is_complete, message_id FROM {self.tableName} WHERE Id = '{id}'") as cursor:        
            item = None

            async for row in cursor:
                item = {}
                item["id"] = row[0]            
                item["version"] = row[1]                        
                item["clientId"] = row[2]                                        
                item["name"] = row[3]
                item["description"] = row[4]
                item["isComplete"] = bool(row[5])
                item["messageId"] = row[6]

        return item            

    async def GetItemByName(self, name):
        asyncDbConn = self.dbConn                

        async with asyncDbConn.execute(f"SELECT id, version, client_id, name, description, is_complete, message_id FROM {self.tableName} WHERE name = '{name}'") as cursor:        
            item = None

            async for row in cursor:
                item = {}
                item["id"] = row[0]            
                item["version"] = row[1]                        
                item["clientId"] = row[2]
                item["name"] = row[3]
                item["description"] = row[4]
                item["isComplete"] = bool(row[5])
                item["messageId"] = row[6]

        return item            

    async def GetItemCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteItem(self, id):
        await super().DeleteEntity(id)        

    async def DeleteAllItems(self):
        await super().DeleteAllEntities()
