from Meter import Meter
from EntityDaoSqlite import EntityDaoSqlite
import json

class MeterDaoSqlite(EntityDaoSqlite):
    def __init__(self, dbConn):
        super().__init__(dbConn)
        self.tableName = "meter"
       
    async def InitDb(self):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute("DROP TABLE IF EXISTS meter_reading")
        await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
        await asyncDbConn.execute("DROP INDEX IF EXISTS index_meter_code")
        await asyncDbConn.execute("DROP INDEX IF EXISTS index_meter_id_client_id")
        await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
            (ID            INTEGER PRIMARY KEY AUTOINCREMENT,
            VERSION        INTEGER NOT NULL,
            CLIENT_ID      INTEGER NOT NULL,                                                               
            MESSAGE_ID     TEXT,                                      
            GUID           TEXT    NOT NULL,                    
            CODE           TEXT    NOT NULL,
            DESCRIPTION    TEXT    NOT NULL,         
            ADR            NUMERIC(19, 4),                                                                                 
            IS_PAUSED      BOOLEAN     NOT NULL);''')
        await asyncDbConn.commit()

    async def AddMeter(self, meter):
        self.entityCount += 1
        lastId = 0        
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"INSERT INTO {self.tableName} (VERSION, CLIENT_ID, GUID, MESSAGE_ID, CODE, DESCRIPTION, IS_PAUSED, ADR) \
            VALUES ({meter['version']}, {meter['clientId']}, '{meter['guid']}', '{meter['messageId']}', '{meter['code']}', '{meter['description']}', {meter['isPaused']}, 0)")

        await asyncDbConn.commit()

        async with asyncDbConn.execute(f"select last_insert_rowid();") as cursor:                
            async for row in cursor:        
                lastId = row[0]

        newAsset = await self.GetMeterById(lastId)                                              
        return newAsset            

    async def UpdateMeter(self, id, meter):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                    SET \
                                    VERSION = VERSION + 1, \
                                    CODE = '{meter['code']}', \
                                    DESCRIPTION = '{meter['description']}', \
                                    IS_PAUSED = {meter['isPaused']}, \
                                    MESSAGE_ID = '{meter['messageId']}' \
                                    WHERE ID = {id}")

        await asyncDbConn.commit()    
        updatedMeter = await self.GetMeterById(id)                     
        return updatedMeter                        

    async def GetMeterById(self, id):
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f"SELECT id, version, client_id, code, description, is_paused, message_id, guid, adr FROM {self.tableName} WHERE Id = '{id}'") as cursor:                
            meter = None

            async for row in cursor:
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

        return meter            

    async def GetAssetByCode(self, code):
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f"SELECT id, version, client_id, code, description, is_paused, message_id, guid, adr FROM {self.tableName} WHERE code= '{code}'") as cursor:                
            meter = None

            async for row in cursor:
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

        return meter            

    async def GetMeterCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteMeter(self, id):
        await super().DeleteEntity(id)

    async def DeleteAllMeters(self):
        await super().DeleteAllEntities()        

    async def GetMeterAdr(self, meterId):
        adr = 0        
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f'''
SELECT AVG(COALESCE(daily_rate, 0)) AS average_daily_rate
FROM (
    SELECT
        (reading - LAG(reading) OVER (ORDER BY reading_on)) * 1.0
        /
        NULLIF(
            julianday(date(reading_on)) -
            julianday(date(LAG(reading_on) OVER (ORDER BY reading_on))),
            0
        ) AS daily_rate
    FROM meter_reading
    WHERE meter_id = {meterId}
) AS daily_rates;
                    ''') as cursor:                
            async for row in cursor:        
                adr = row[0]

        return float(adr)        
