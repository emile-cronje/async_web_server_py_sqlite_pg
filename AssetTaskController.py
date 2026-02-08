from AssetTask import AssetTask
import json

class LowercaseJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        # Convert dictionary keys to lowercase
        obj = {key.lower(): value for key, value in obj.items()}
        return super().encode(obj)

class AssetTaskController:
    def __init__(self, mqttConnectionPool, dao, mqttQueuePool, useQ = False):
        self.assetTaskDao = dao
        self.mqttConnectionPool = mqttConnectionPool
        self.useQ = useQ        
        self.mqttQueuePool = mqttQueuePool
        
    async def publishMsg(self, message):
        if (self.useQ == True):
            pubQ = self.mqttQueuePool.GetPubQ()                        
            await pubQ.put(message)                            

    async def AddAssetTask(self, mqttSessionId, assetTask):
        assetTask["version"] = 0
        clientId = assetTask["clientId"]        

        savedAssetTask = await self.assetTaskDao.AddAssetTask(assetTask)

        json_string = json.dumps(savedAssetTask, cls=LowercaseJSONEncoder, indent=4)
        entityId = savedAssetTask["id"]

        if (savedAssetTask != None):
            asset_task_data = {
                            "MqttSessionId": mqttSessionId,                                                                                            
                            "MessageId": savedAssetTask["messageId"],                                                            
                            "ClientId": clientId,                                            
                            "EntityType":"AssetTask",
                            "Operation":"Create",                                                
                            "Entity" : json_string,
                            "entityId": entityId
                        }
            
            await self.publishMsg(json.dumps(asset_task_data))
            
        return savedAssetTask

    async def UpdateAssetTask(self, mqttSessionId, id, updatedAssetTask):
        savedAssetTask = await self.GetAssetTaskById(id)

        if (savedAssetTask == None):        
            return None
        else:
            savedAssetTask["code"] = str(updatedAssetTask["code"])
            savedAssetTask["description"] = str(updatedAssetTask["description"])
            savedAssetTask["isRfs"] = bool(updatedAssetTask["isRfs"])
            savedAssetTask["messageId"] = updatedAssetTask["messageId"]                            
            savedAssetTask = await self.assetTaskDao.UpdateAssetTask(id, savedAssetTask)                
            
            if (savedAssetTask != None):            
                json_string = json.dumps(savedAssetTask, cls=LowercaseJSONEncoder, indent=4)                            
                asset_task_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                    
                                "MessageId": savedAssetTask["messageId"],                                                                                
                                "ClientId": savedAssetTask['clientId'],
                                "EntityType":"AssetTask",
                                "Operation":"Update",                        
                                "Entity" : json_string
                             }
            
                await self.publishMsg(json.dumps(asset_task_data))                    
            
        return savedAssetTask

    async def GetAssetTaskById(self, id):
        assetTask = await self.assetTaskDao.GetAssetTaskById(id)                        
        return assetTask        

    async def GetAllAssetTasks(self, clientId):
        result = await self.assetTaskDao.GetAllAssetTasks(clientId)
        return result

    async def DeleteAssetTask(self, id, mqttSessionId, messageId):
        savedAssetTask = await self.GetAssetTaskById(id)

        if (savedAssetTask == None):        
            asset_task_data = {
                            "MqttSessionId": mqttSessionId,                                                                                                                                                
                            "MessageId": messageId,                                                                                
                            "ClientId": 0,
                            "EntityType":"AssetTask",
                            "Operation":"Delete"                        
                        }

            await self.publishMsg(json.dumps(asset_task_data))                    
            return "Asset Task not found"
        else:
            await self.assetTaskDao.DeleteAssetTask(id)
            json_string = json.dumps(savedAssetTask, cls=LowercaseJSONEncoder, indent=4)            
            
            if (savedAssetTask != None):            
                asset_task_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                                    
                                "MessageId": messageId,                                                                                
                                "ClientId": savedAssetTask["clientId"],                                                                                      
                                "EntityType":"AssetTask",
                                "Operation":"Delete",                        
                                "Entity" : json_string,
                                "EntityId" : savedAssetTask["id"]
                             }
            
                await self.publishMsg(json.dumps(asset_task_data))                    

        return "Delete OK"            

    async def DeleteAllAssetTasks(self):
        result = await self.assetTaskDao.DeleteAllAssetTasks()
        return result

    async def DeleteAssetTasksForAsset(self, mqttSessionId, messageId, assetId):
        taskIds = await self.assetTaskDao.GetTaskIdsForAsset(assetId)

        for taskId in taskIds:
            await self.DeleteAssetTask(taskId, mqttSessionId, messageId)

    async def GetAssetTaskCount(self):
        result = await self.assetTaskDao.GetAssetTaskCount()
        return result
