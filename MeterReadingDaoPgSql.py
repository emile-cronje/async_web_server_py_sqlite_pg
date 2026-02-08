import json
from MeterReading import MeterReading
from EntityDaoPg import EntityDaoPg
from datetime import datetime

class MeterReadingDaoPgSql(EntityDaoPg):
    def __init__(self, meterDao):
        self.tableName = "meter_reading"
        super().__init__(self.tableName)        
        self.meterDao = meterDao        
       
    async def InitDb(self):
        await super().InitDb()        
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DROP TABLE IF EXISTS {self.tableName}")

            await asyncDbConn.execute(f'''CREATE TABLE {self.tableName}
                    (ID            SERIAL PRIMARY KEY,
                    VERSION        INTEGER NOT NULL,
                    CLIENT_ID      INTEGER NOT NULL,                                                              
                    MESSAGE_ID     TEXT,                                                                                                                  
                    METER_ID       BIGINT  NOT NULL,
                    READING        DECIMAL,
                    READING_ON     TIMESTAMP, CONSTRAINT fk_meter FOREIGN KEY(METER_ID) REFERENCES METER(ID));''')
           
        await self.ReleaseDbConnection(asyncDbConn)

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
        asyncDbConn = await self.GetDbConnection()

        meter = await self.meterDao.GetMeterById(meterReading['meterId'])    

        if (meter == None):
            return None

        lastId = 0        
        readingOnString = self.fix_isoformat_string(meterReading['readingOn'])        
        readingOnString = readingOnString.replace('T', ' ')                
        readingOnString = readingOnString.replace('+00:00', '')                        
        readingOn = datetime.fromisoformat(readingOnString)        
   
        async with asyncDbConn.transaction():        
            lastId = await asyncDbConn.fetchval(f"INSERT INTO {self.tableName} (VERSION, CLIENT_ID, METER_ID, READING, READING_ON, MESSAGE_ID) \
                VALUES ($1, $2, $3, $4, $5, $6) RETURNING ID", meterReading['version'], int(meterReading['clientId']), meterReading['meterId'], meterReading['reading'], readingOn, str(meterReading['messageId']))

        await self.ReleaseDbConnection(asyncDbConn)

        newMeterReading = await self.GetMeterReadingById(lastId)
        return newMeterReading

    async def UpdateMeterReading(self, id, meterReading):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():        
            await asyncDbConn.execute(f"UPDATE {self.tableName} \
                                    SET \
                                    VERSION = VERSION + 1, \
                                    READING = {meterReading['reading']}, \
                                    READING_ON = '{meterReading['readingOn']}', \
                                    MESSAGE_ID = '{meterReading['messageId']}' \
                                    WHERE ID = {id}")

        await self.ReleaseDbConnection(asyncDbConn)
        updatedMeterReading = await self.GetMeterReadingById(id)                             

        return updatedMeterReading

    async def GetMeterReadingById(self, id):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            sql = f"SELECT id, version, client_id, reading, reading_on, meter_id, message_id FROM {self.tableName} WHERE ID = {id}"

            row = await asyncDbConn.fetchrow(sql)
            meterReading = None

            if row:
                meterReading = {}                
                meterReading['id'] = row[0]            
                meterReading['version'] = row[1]                        
                meterReading['clientId'] = row[2]                                                            
                meterReading['reading'] = float(row[3])
                meterReading['readingOn'] = datetime.isoformat(row[4])
                meterReading['meterId'] = row[5]                                                                            
                meterReading['messageId'] = row[6]                                                                                            

        await self.ReleaseDbConnection(asyncDbConn)
        return meterReading            
    
    async def GetMeterReadingCount(self):
        count = await super().GetEntityCount()
        return count

    async def DeleteMeterReadingsForMeter(self, meterId):
        asyncDbConn = await self.GetDbConnection()

        async with asyncDbConn.transaction():
            await asyncDbConn.execute(f"DELETE FROM {self.tableName} WHERE meter_id = {meterId}")                    

        await self.ReleaseDbConnection(asyncDbConn)

    async def DeleteMeterReading(self, id):
        await super().DeleteEntity(id)                

    async def DeleteAllMeterReadings(self):
        await super().DeleteAllEntities()

    async def GetMeterReadingIdsForMeter(self, meterId):
        asyncDbConn = await self.GetDbConnection()

        rows = await asyncDbConn.fetch(f"SELECT ID FROM {self.tableName} WHERE meter_id = {meterId}")
        ids = [row['id'] for row in rows]        

        await self.ReleaseDbConnection(asyncDbConn)

        return ids
