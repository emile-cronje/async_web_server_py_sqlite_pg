from Asset import Asset
import json

class LowercaseJSONEncoder(json.JSONEncoder):
    def encode(self, obj): # type: ignore
        # Convert dictionary keys to lowercase
        obj = {key.lower(): value for key, value in obj.items()}
        return super().encode(obj)

class AssetController:
    def __init__(self, mqttConnectionPool, dao, mqttQueuePool, assetTaskController, useQ = False):
        self.assetDao = dao
        self.mqttConnectionPool = mqttConnectionPool
        self.useQ = useQ        
        self.mqttQueuePool = mqttQueuePool
        self.assetTaskController = assetTaskController
        
    async def publishMsg(self, message):
        if (self.useQ == True):
            pubQ = self.mqttQueuePool.GetPubQ()                        
            await pubQ.put(message)                            

    async def AddAsset(self, mqttSessionId, asset):
        asset["version"] = 0
        clientId = asset["clientId"]        

        savedAsset = await self.assetDao.AddAsset(asset)

        json_string = json.dumps(savedAsset, cls=LowercaseJSONEncoder, indent=4)
        entityId = savedAsset["id"]

        if (savedAsset != None):
            asset_data = {
                            "MqttSessionId": mqttSessionId,                                                                                            
                            "MessageId": savedAsset["messageId"],                                                            
                            "ClientId": clientId,                                            
                            "EntityType":"Asset",
                            "Operation":"Create",                                                
                            "Entity" : json_string,
                            "entityId": entityId
                        }
            
            await self.publishMsg(json.dumps(asset_data))
            
        return savedAsset

    async def UpdateAsset(self, mqttSessionId, id, updatedAsset):
        savedAsset = await self.GetAssetById(id)

        if (savedAsset == None):        
            return None
        else:
            savedAsset["code"] = str(updatedAsset["code"])
            savedAsset["description"] = str(updatedAsset["description"])
            savedAsset["isMsi"] = bool(updatedAsset["isMsi"])
            savedAsset["messageId"] = updatedAsset["messageId"]                            
            savedAsset = await self.assetDao.UpdateAsset(id, savedAsset)                
            
            if (savedAsset != None):            
                json_string = json.dumps(savedAsset, cls=LowercaseJSONEncoder, indent=4)                            
                asset_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                    
                                "MessageId": savedAsset["messageId"],                                                                                
                                "ClientId": savedAsset['clientId'],
                                "EntityType":"Asset",
                                "Operation":"Update",                        
                                "Entity" : json_string
                             }
            
                await self.publishMsg(json.dumps(asset_data))                    
            
        return savedAsset

    async def GetAssetById(self, id):
        asset = await self.assetDao.GetAssetById(id)                        
        return asset        

    async def GetAllAssets(self, clientId):
        result = await self.assetDao.GetAllAssets(clientId)
        return result

    async def DeleteAllAssets(self):
        result = await self.assetDao.DeleteAllAssets()
        return result

    async def DeleteAsset(self, id, mqttSessionId, messageId):
        savedAsset = await self.GetAssetById(id)

        if (savedAsset == None):        
            asset_data = {
                            "MqttSessionId": mqttSessionId,                                                                                                                                                
                            "MessageId": messageId,                                                                                
                            "ClientId": 0,
                            "EntityType":"Asset",
                            "Operation":"Delete"                        
                        }

            await self.publishMsg(json.dumps(asset_data))                    
            return "Asset not found"
        else:
            await self.assetTaskController.DeleteAssetTasksForAsset(mqttSessionId, messageId, id)            
            await self.assetDao.DeleteAsset(id)
            json_string = json.dumps(savedAsset, cls=LowercaseJSONEncoder, indent=4)            
            
            if (savedAsset != None):            
                asset_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                                    
                                "MessageId": messageId,                                                                                
                                "ClientId": savedAsset["clientId"],                                                                                      
                                "EntityType":"Asset",
                                "Operation":"Delete",                        
                                "Entity" : json_string,
                                "EntityId" : savedAsset["id"]
                             }
            
                await self.publishMsg(json.dumps(asset_data))                    

        return "Delete OK"            

    async def GetAssetCount(self):
        result = await self.assetDao.GetAssetCount()
        return result
