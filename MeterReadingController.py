from MeterReading import MeterReading
import json

class LowercaseJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        # Convert dictionary keys to lowercase
        obj = {key.lower(): value for key, value in obj.items()}
        return super().encode(obj)

class MeterReadingController:
    def __init__(self, mqttConnectionPool, dao, mqttQueuePool, useQ = False):
        self.meterReadingDao = dao
        self.mqttConnectionPool = mqttConnectionPool
        self.useQ = useQ        
        self.mqttQueuePool = mqttQueuePool
        
    async def publishMsg(self, message):
        if (self.useQ == True):
            pubQ = self.mqttQueuePool.GetPubQ()                        
            await pubQ.put(message)                            

    async def AddMeterReading(self, mqttSessionId, meterReading):
        meterReading["version"] = 0
        clientId = meterReading["clientId"]        

        savedMeterReading = await self.meterReadingDao.AddMeterReading(meterReading)

        json_string = json.dumps(savedMeterReading, cls=LowercaseJSONEncoder, indent=4)
        entityId = savedMeterReading["id"]

        if (savedMeterReading != None):
            asset_task_data = {
                            "MqttSessionId": mqttSessionId,                                                                                            
                            "MessageId": savedMeterReading["messageId"],                                                            
                            "ClientId": clientId,                                            
                            "EntityType":"MeterReading",
                            "Operation":"Create",                                                
                            "Entity" : json_string,
                            "entityId": entityId
                        }
            
            await self.publishMsg(json.dumps(asset_task_data))
            
        return savedMeterReading

    async def UpdateMeterReading(self, mqttSessionId, id, updatedMeterReading):
        savedMeterReading = await self.GetMeterReadingById(id)

        if (savedMeterReading == None):        
            return None
        else:
            savedMeterReading["reading"] = updatedMeterReading["reading"]
            savedMeterReading["readingOn"] = updatedMeterReading["readingOn"]
            savedMeterReading["messageId"] = updatedMeterReading["messageId"]                            
            savedMeterReading = await self.meterReadingDao.UpdateMeterReading(id, savedMeterReading)                
            
            if (savedMeterReading != None):            
                json_string = json.dumps(savedMeterReading, cls=LowercaseJSONEncoder, indent=4)                            
                reading_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                    
                                "MessageId": savedMeterReading["messageId"],                                                                                
                                "ClientId": savedMeterReading['clientId'],
                                "EntityType":"MeterReading",
                                "Operation":"Update",                        
                                "Entity" : json_string
                             }
            
                await self.publishMsg(json.dumps(reading_data))                    
            
        return savedMeterReading

    async def GetMeterReadingById(self, id):
        return await self.meterReadingDao.GetMeterReadingById(id)

    async def GetAllMeterReadings(self, clientId):
        result = await self.meterReadingDao.GetAllMeterReadings(clientId)
        return result

    async def DeleteAllMeterReadings(self):
        result = await self.meterReadingDao.DeleteAllMeterReadings()
        return result

    async def DeleteMeterReading(self, id, mqttSessionId, messageId):
        savedMeterReading = await self.GetMeterReadingById(id)

        if (savedMeterReading == None):        
            reading_data = {
                            "MqttSessionId": mqttSessionId,                                                                                                                                                
                            "MessageId": messageId,                                                                                
                            "ClientId": 0,
                            "EntityType":"MeterReading",
                            "Operation":"Delete"                        
                        }

            await self.publishMsg(json.dumps(reading_data))                    
            return "Meter Reading not found"
        else:
            await self.meterReadingDao.DeleteMeterReading(id)
            json_string = json.dumps(savedMeterReading, cls=LowercaseJSONEncoder, indent=4)            
            
            if (savedMeterReading != None):            
                reading_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                                    
                                "MessageId": messageId,                                                                                
                                "ClientId": savedMeterReading["clientId"],                                                                                      
                                "EntityType":"MeterReading",
                                "Operation":"Delete",                        
                                "Entity" : json_string,
                                "EntityId" : savedMeterReading["id"]
                             }
            
                await self.publishMsg(json.dumps(reading_data))                    

        return "Delete OK"            

    async def GetMeterReadingCount(self):
        result = await self.meterReadingDao.GetMeterReadingCount()
        return result

    async def DeleteMeterReadingsForMeter(self, mqttSessionId, messageId, meterId):
        meterReadingIds = await self.meterReadingDao.GetMeterReadingIdsForMeter(meterId)

        for readingId in meterReadingIds:
            await self.DeleteMeterReading(readingId, mqttSessionId, messageId)
