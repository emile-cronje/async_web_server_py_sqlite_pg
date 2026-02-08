from MeterReading import MeterReading
from EntityDaoSqlite import EntityDaoSqlite
import json
from datetime import datetime

class MeterReadingDaoSqlite(EntityDaoSqlite):
    def __init__(self, dbConn):
        super().__init__(dbConn)
        self.tableName = "meter_reading"
       
    async def InitDb(self):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")
        await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
                (ID            INTEGER PRIMARY KEY AUTOINCREMENT,
                VERSION        INTEGER NOT NULL,
            CLIENT_ID      INTEGER NOT NULL,                                                               
            MESSAGE_ID     TEXT,                                                                                                                  
            METER_ID       BIGINT  NOT NULL,
            READING        DECIMAL,
            READING_ON     TIMESTAMP,
            CONSTRAINT fk_meter FOREIGN KEY(METER_ID) REFERENCES meter(ID));''')

        await asyncDbConn.commit()

    def fix_isoformat_string(self, iso_string):
        # Replace 'Z' with '+00:00' for UTC
        if iso_string.endswith('Z'):
            iso_string = iso_string.replace('Z', '+00:00')
        
        # Find the fractional seconds part and limit to 6 digits
        if '.' in iso_string:
            # Split the datetime string on the decimal point
            date_part, frac_part = iso_string.split('.')
            # Split the fractional part by the timezone offset
            frac_seconds, tz_part = frac_part.split('+') if '+' in frac_part else frac_part.split('-')
            # Truncate the fractional seconds to 6 digits
            frac_seconds = frac_seconds[:6]
            # Reassemble the ISO string with truncated precision and correct timezone
            iso_string = f"{date_part}.{frac_seconds}+{tz_part}"
    
        return iso_string

    async def AddMeterReading(self, meterReading):
        self.entityCount += 1
        lastId = 0        
        asyncDbConn = self.dbConn

        lastId = 0        
        readingOnString = self.fix_isoformat_string(meterReading['readingOn'])        
        readingOnString = readingOnString.replace('T', ' ')                
        readingOn = datetime.fromisoformat(readingOnString)        

        await asyncDbConn.execute(f"INSERT INTO {self.tableName} (VERSION, CLIENT_ID, METER_ID, READING, READING_ON, MESSAGE_ID) \
            VALUES (?, ?, ?, ?, ?, ?)", (meterReading['version'], meterReading['clientId'], meterReading['meterId'], meterReading['reading'], readingOn, meterReading['messageId']))

        await asyncDbConn.commit()

        async with asyncDbConn.execute(f"select last_insert_rowid();") as cursor:                
            async for row in cursor:        
                lastId = row[0]

        newMeterReading = await self.GetMeterReadingById(lastId)                                              
        return newMeterReading            

    async def UpdateMeterReading(self, id, meterReading):
        asyncDbConn = self.dbConn

        await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                    SET \
                                    VERSION = VERSION + 1, \
                                    READING = '{meterReading['reading']}', \
                                    READING_ON = '{meterReading['readingOn']}', \
                                    MESSAGE_ID = '{meterReading['messageId']}' \
                                    WHERE ID = {id}")

        await asyncDbConn.commit()    
        updatedMeterReading = await self.GetMeterReadingById(id)                     
        return updatedMeterReading                        

    async def GetMeterReadingById(self, id):
        asyncDbConn = self.dbConn

        async with asyncDbConn.execute(f"SELECT id, version, client_id, meter_id, reading, reading_on, message_id FROM {self.tableName} WHERE Id = '{id}'") as cursor:                
            meterReading = None

            async for row in cursor:
                meterReading = {}
                meterReading["id"] = row[0]            
                meterReading["version"] = row[1]                        
                meterReading["clientId"] = row[2]                                                        
                meterReading["meterId"] = row[3]                                                                        
                meterReading["reading"] = row[4]
                meterReading["readingOn"] = row[5]
                meterReading["messageId"] = row[6]

        return meterReading            

    async def GetMeterReadingCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteMeterReading(self, id):
        await super().DeleteEntity(id)

    async def GetMeterReadingIdsForMeter(self, meterId):
        asyncDbConn = self.dbConn
        ids = []

        async with asyncDbConn.execute(f"SELECT ID FROM {self.tableName} WHERE meter_id = {meterId}") as cursor:
            async for row in cursor:
                ids.append(row[0])

        return ids

    async def DeleteAllMeterReadings(self):
        await super().DeleteAllEntities()        
