from ToDoItem import ToDoItem
import json

class LowercaseJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        # Convert dictionary keys to lowercase
        obj = {key.lower(): value for key, value in obj.items()}
        return super().encode(obj)
    
class ToDoController:
    def __init__(self, mqttConnectionPool, dao, mqttQueuePool, useQ = False):
        self.todoDao = dao
        self.mqttConnectionPool = mqttConnectionPool
        self.useQ = useQ
        self.mqttQueuePool = mqttQueuePool
        self.mqttConnection = None

    async def publishMsg(self, message):
        if (self.useQ == True):
            pubQ = self.mqttQueuePool.GetPubQ()            
            await pubQ.put(message)                            
        else:
            mqttConnection = self.mqttConnectionPool['192.168.10.135']            

            if (mqttConnection != None):            
                await mqttConnection.publish("/entities", message, qos = 1)                        

    async def AddItem(self, mqttSessionId, item):
        item["version"] = 0
        clientId = item["clientId"]        

        savedToDoItem = await self.todoDao.AddItem(item)

        json_string = json.dumps(savedToDoItem, cls=LowercaseJSONEncoder, indent=4)
        entityId = savedToDoItem["id"]

        if (savedToDoItem != None):
            item_data = {
                            "MqttSessionId": mqttSessionId,                                                                            
                            "MessageId": savedToDoItem["messageId"],                                                            
                            "ClientId": clientId,                                            
                            "EntityType":"ToDoItem",
                            "Operation":"Create",                                                
                            "Entity" : json_string,
                            "entityId": entityId
                        }
            
        await self.publishMsg(json.dumps(item_data))
            
        return savedToDoItem

    async def UpdateItem(self, mqttSessionId, id, updatedItem):
        savedItem = await self.GetItemById(id)

        if (savedItem == None):        
            return None
        else:
            savedItem["name"] = str(updatedItem["name"])
            savedItem["description"] = str(updatedItem["description"])
            savedItem["isComplete"] = bool(updatedItem["isComplete"])
            savedItem["messageId"] = updatedItem["messageId"]                            
            savedItem = await self.todoDao.UpdateItem(id, savedItem)                
            
            if (savedItem != None):            
                json_string = json.dumps(savedItem, cls=LowercaseJSONEncoder, indent=4)                            
                item_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                
                                "MessageId": savedItem["messageId"],                                                                                
                                "ClientId": savedItem['clientId'],
                                "EntityType":"ToDoItem",
                                "Operation":"Update",                        
                                "Entity" : json_string
                             }
            
                await self.publishMsg(json.dumps(item_data))                    
            
        return savedItem

    async def DeleteItem(self, id, mqttSessionId, messageId):
        savedItem = await self.GetItemById(id)

        if (savedItem == None):        
            item_data = {
                            "MqttSessionId": mqttSessionId,                                                                                                                                
                            "MessageId": messageId,                                                                                
                            "ClientId": 0,
                            "EntityType":"ToDoItem",
                            "Operation":"Delete"                        
                        }

            await self.publishMsg(json.dumps(item_data))                    
            #print("Delete: Item not found: " + str(messageId))            
            return "Item not found"
        else:
            await self.todoDao.DeleteItem(id)
            json_string = json.dumps(savedItem, cls=LowercaseJSONEncoder, indent=4)            
            
            if (savedItem != None):            
                item_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                                    
                                "MessageId": messageId,                                                                                
                                "ClientId": savedItem["clientId"],                                                                                      
                                "EntityType":"ToDoItem",
                                "Operation":"Delete",                        
                                "Entity" : json_string,
                                "EntityId" : savedItem["id"]
                             }
            
            await self.publishMsg(json.dumps(item_data))                    

        return "Delete OK"            

    async def GetItemById(self, id):
        item = await self.todoDao.GetItemById(id)                        
        return item        

    async def GetAllItems(self):
        result = await self.todoDao.GetAllItems()            
        return result

    async def DeleteAllItems(self):
        result = await self.todoDao.DeleteAllItems()
        return result

    async def GetItemCount(self):
        result = await self.todoDao.GetItemCount()
        return result
