from Meter import Meter
import json

class LowercaseJSONEncoder(json.JSONEncoder):
    def encode(self, obj): # type: ignore
        obj = {key.lower(): value for key, value in obj.items()}
        return super().encode(obj)

class MeterController:
    def __init__(self, mqttConnectionPool, dao, mqttQueuePool, meterReadingController, useQ = False):
        self.meterDao = dao
        self.mqttConnectionPool = mqttConnectionPool
        self.useQ = useQ        
        self.mqttQueuePool = mqttQueuePool
        self.meterReadingController = meterReadingController        
        
    async def publishMsg(self, message):
        if (self.useQ == True):
            pubQ = self.mqttQueuePool.GetPubQ()                        
            await pubQ.put(message)                            

    async def AddMeter(self, mqttSessionId, meter):
        meter["version"] = 0
        clientId = meter["clientId"]        

        savedMeter = await self.meterDao.AddMeter(meter)

        json_string = json.dumps(savedMeter, cls=LowercaseJSONEncoder, indent=4)
        entityId = savedMeter["id"]

        if (savedMeter != None):
            meter_data = {
                            "MqttSessionId": mqttSessionId,                                                                                                            
                            "MessageId": savedMeter["messageId"],                                                            
                            "ClientId": clientId,                                            
                            "EntityType":"Meter",
                            "Operation":"Create",                                                
                            "Entity" : json_string,
                            "entityId": entityId
                        }
            
            await self.publishMsg(json.dumps(meter_data))
            
        return savedMeter

    async def UpdateMeter(self, mqttSessionId, id, updatedMeter):
        savedMeter = await self.GetMeterById(id)

        if (savedMeter == None):        
            return None
        else:
            savedMeter["code"] = str(updatedMeter["code"])
            savedMeter["description"] = str(updatedMeter["description"])
            savedMeter["isPaused"] = bool(updatedMeter["isPaused"])
            savedMeter["messageId"] = updatedMeter["messageId"]                            
            savedMeter = await self.meterDao.UpdateMeter(id, savedMeter)                
            
            if (savedMeter != None):            
                json_string = json.dumps(savedMeter, cls=LowercaseJSONEncoder, indent=4)                            
                meter_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                
                                "MessageId": savedMeter["messageId"],                                                                                
                                "ClientId": savedMeter['clientId'],
                                "EntityType":"Meter",
                                "Operation":"Update",                        
                                "Entity" : json_string
                             }
            
                await self.publishMsg(json.dumps(meter_data))                    
            
        return savedMeter

    async def GetMeterById(self, id):
        meter = await self.meterDao.GetMeterById(id)                        
        return meter        

    async def GetAllMeters(self, clientId):
        result = await self.meterDao.GetAllMeters(clientId)
        return result

    async def DeleteAllMeters(self):
        result = await self.meterDao.DeleteAllMeters()
        return result

    async def DeleteMeter(self, id, mqttSessionId, messageId):
        savedMeter = await self.GetMeterById(id)

        if (savedMeter == None):        
            meter_data = {
                            "MqttSessionId": mqttSessionId,                                                                                                                                                                
                            "MessageId": messageId,                                                                                
                            "ClientId": 0,
                            "EntityType":"Meter",
                            "Operation":"Delete"                        
                        }

            await self.publishMsg(json.dumps(meter_data))                    
            return "Meter not found"
        else:
            await self.meterReadingController.DeleteMeterReadingsForMeter(mqttSessionId, messageId, id)        
            await self.meterDao.DeleteMeter(id)
            json_string = json.dumps(savedMeter, cls=LowercaseJSONEncoder, indent=4)            
            
            if (savedMeter != None):            
                meter_data = {
                                "MqttSessionId": mqttSessionId,                                                                                                                                                                        
                                "MessageId": messageId,                                                                                
                                "ClientId": savedMeter["clientId"],                                                                                      
                                "EntityType":"Meter",
                                "Operation":"Delete",                        
                                "Entity" : json_string,
                                "EntityId" : savedMeter["id"]
                             }
            
                await self.publishMsg(json.dumps(meter_data))                    

        return "Delete OK"            

    async def GetMeterCount(self):
        result = await self.meterDao.GetMeterCount()
        return result
    
    async def GetMeterAdr(self, id):
        result = await self.meterDao.GetMeterAdr(id)
        return result

