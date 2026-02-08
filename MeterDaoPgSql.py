import json
from Meter import Meter
from EntityDaoPg import EntityDaoPg

class MeterDaoPgSql(EntityDaoPg):
    def __init__(self):
        self.tableName = "meter"
        super().__init__(self.tableName)        
       
    async def InitDb(self):
        await super().InitDb()        
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DROP TABLE IF EXISTS meter_reading")            
            await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
            await asyncDbConn.execute(f"DROP INDEX IF EXISTS index_meter_code")            
            await asyncDbConn.execute(f"DROP INDEX IF EXISTS index_meter_id_client_id")                        

            await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
                    (ID            SERIAL PRIMARY KEY,
                    VERSION        INTEGER NOT NULL,
                    CLIENT_ID      INTEGER NOT NULL,                                                              
                    MESSAGE_ID     TEXT,                                      
                    GUID           TEXT    NOT NULL,                    
                    CODE           TEXT    NOT NULL,
                    DESCRIPTION    TEXT    NOT NULL,         
                    ADR            NUMERIC(19, 4),                                                                                 
                    IS_PAUSED      BOOL     NOT NULL);''')            
            
            #await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_meter_code
                                #ON {self.tableName}(code)''')
            #await asyncDbConn.execute(f'''CREATE UNIQUE INDEX index_meter_id_client_id
                                #ON {self.tableName}(id, client_id)''')
            
        await self.ReleaseDbConnection(asyncDbConn)
        await self.CreateFunctions()

    async def CreateFunctions(self):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f'''
CREATE OR REPLACE FUNCTION calculate_average_daily_rate(p_meter_id INTEGER)
RETURNS NUMERIC AS $$
DECLARE
avg_daily_rate NUMERIC;
BEGIN
-- Calculate the average daily rate
SELECT AVG(COALESCE(daily_rate, 0)) INTO avg_daily_rate  -- Replace NULL with 0 for averaging
FROM (
SELECT
(reading - LAG(reading) OVER (ORDER BY reading_on))::NUMERIC /
NULLIF((reading_on::DATE - LAG(reading_on) OVER (ORDER BY reading_on)::DATE), 0) AS daily_rate
FROM meter_reading
WHERE meter_id = p_meter_id
ORDER BY id -- consider if you need this ORDER BY, it might not be required and impacts performance
) AS daily_rates;

   -- Return the calculated average daily rate
   RETURN avg_daily_rate;
END;
$$ LANGUAGE plpgsql;
''')
            
        await self.ReleaseDbConnection(asyncDbConn)            

    async def AddMeter(self, meter):
        asyncDbConn = await self.GetDbConnection()
        lastId = 0

        async with asyncDbConn.transaction():        
            lastId = await asyncDbConn.fetchval(f"INSERT INTO {self.tableName} (VERSION, CLIENT_ID, GUID, MESSAGE_ID, CODE, DESCRIPTION, IS_PAUSED, ADR) \
                            VALUES ({meter['version']}, {meter['clientId']}, '{meter['guid']}','{meter['messageId']}', '{meter['code']}', '{meter['description']}', {meter['isPaused']}, 0) RETURNING ID")
            
        await self.ReleaseDbConnection(asyncDbConn)

        newMeter = await self.GetMeterById(lastId)
        return newMeter 

    async def UpdateMeter(self, id, meter):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():        
            await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                        SET \
                                        VERSION = VERSION + 1, \
                                        CODE = '{meter['code']}', \
                                        DESCRIPTION = '{meter['description']}', \
                                        IS_PAUSED = {meter['isPaused']}, \
                                        MESSAGE_ID = '{meter['messageId']}' \
                                        WHERE ID = {id}")

        await self.ReleaseDbConnection(asyncDbConn)

        updatedMeter = await self.GetMeterById(id)                     
        return updatedMeter

    async def GetMeterById(self, id):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            sql = f"SELECT id, version, client_id, code, description, is_paused, message_id, guid, adr FROM {self.tableName} WHERE ID = {id}"

            row = await asyncDbConn.fetchrow(sql)
            meter = None

            if row:
                meter = {}                
                meter["id"] = row[0]            
                meter["version"] = row[1]                        
                meter["clientId"] = row[2]                                                            
                meter["code"] = row[3]
                meter["description"] = row[4]
                meter["isPaused"] = bool(row[5])
                meter["messageId"] = row[6]                
                meter["guid"] = row[7]                                
                meter["adr"] = str(row[8])                                                

        await self.ReleaseDbConnection(asyncDbConn)
        return meter            
   
    async def GetMeterCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteMeter(self, id):
        await super().DeleteEntity(id)                

    async def DeleteAllMeters(self):
        await super().DeleteAllEntities()

    async def GetMeterAdr(self, meterId):
        asyncDbConn = await self.GetDbConnection()
        adr = 0        

        async with asyncDbConn.transaction():        
            adr = await asyncDbConn.fetchval(f"SELECT COALESCE(calculate_average_daily_rate({meterId}), 0);")

        await self.ReleaseDbConnection(asyncDbConn)

        return float(adr)        