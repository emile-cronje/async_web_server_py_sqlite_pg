import json
from uuid import uuid4
import uuid
from aiohttp import web
import sqlite3
import aiosqlite
from binascii import a2b_base64 as base64_decode
from base64 import b64encode
from ToDoController import ToDoController
from ToDoDaoSqlite import ToDoDaoSqlite
from AssetController import AssetController
from MeterController import MeterController
from MeterReadingController import MeterReadingController
from AssetTaskController import AssetTaskController
from AssetDaoSqlite import AssetDaoSqlite
from MeterDaoSqlite import MeterDaoSqlite
from MeterReadingDaoSqlite import MeterReadingDaoSqlite
from AssetTaskDaoSqlite import AssetTaskDaoSqlite
import aiomqtt
import asyncio
import sys, os
from mqttQueuePool import MqttQueuePool
from AssetTaskDaoPgSql import AssetTaskDaoPgSql
from AssetDaoPgSql import AssetDaoPgSql
from ToDoDaoPgSql import ToDoDaoPgSql
from MeterDaoPgSql import MeterDaoPgSql
from MeterReadingDaoPgSql import MeterReadingDaoPgSql
import asyncpg

class WebServer:
    def __init__(self, **kwargs: dict):
        self.app = web.Application()
        self.host = kwargs['webserver']['host']
        self.port = kwargs['webserver']['port']
        self.databases = kwargs['databases']
        self.mqttPubClient = None
        self.dbConnPool = None        
        self.sessionToUser = {}
        self.userToSession = {}
        self.toDoController = None
        self.toDoDao = None
        self.assetController = None
        self.meterController = None        
        self.assetDao = None
        self.meterDao = None        
        self.meterReadingDao = None                
        self.assetTaskController = None
        self.assetTaskDao = None
        self.mqttBroker = kwargs['mqttbroker']['host']        
        self.mqttClientId = kwargs['mqttbroker']['client_id']
        self.mqttListener = 'mqttListener'
        self.mqttBrokers = None
        self.mqttQueuePool = MqttQueuePool()        
        self.mqttConnectionPool = {}                
        self.useQ = True

    async def InitDao(self, app):
        dbIsPg = False

        if (dbIsPg == True):
            self.toDoDao = ToDoDaoPgSql()        
            self.assetDao = AssetDaoPgSql()                    
            self.assetTaskDao = AssetTaskDaoPgSql(self.assetDao)                                
            self.meterDao = MeterDaoPgSql()                                            
            self.meterReadingDao = MeterReadingDaoPgSql(self.meterDao)                                                        

            await self.toDoDao.InitDb()            
            await self.assetDao.InitDb()                            
            await self.assetTaskDao.InitDb()                
            await self.meterDao.InitDb()                                        
            await self.meterReadingDao.InitDb()
        else:
            # ToDoItems
            self.dbConnPool = await self.InitSqliteDb('test.db')            
            self.toDoDao = ToDoDaoSqlite(self.dbConnPool)
            await self.toDoDao.InitDb()                        
            self.assetDao = AssetDaoSqlite(self.dbConnPool)            
            await self.assetDao.InitDb()                                
            self.assetTaskDao = AssetTaskDaoSqlite(self.dbConnPool)            
            await self.assetTaskDao.InitDb()                                        
            self.meterDao = MeterDaoSqlite(self.dbConnPool)            
            await self.meterDao.InitDb()                                        
            self.meterReadingDao = MeterReadingDaoSqlite(self.dbConnPool)            
            await self.meterReadingDao.InitDb()                                        

        self.toDoController = ToDoController(self.mqttConnectionPool, self.toDoDao, self.mqttQueuePool, useQ=self.useQ)
        self.assetTaskController = AssetTaskController(self.mqttConnectionPool, self.assetTaskDao, self.mqttQueuePool, useQ=self.useQ)        
        self.assetController = AssetController(self.mqttConnectionPool, self.assetDao, self.mqttQueuePool, self.assetTaskController, useQ=self.useQ)        
        self.meterReadingController = MeterReadingController(self.mqttConnectionPool, self.meterReadingDao, self.mqttQueuePool, useQ=self.useQ)                        
        self.meterController = MeterController(self.mqttConnectionPool, self.meterDao, self.mqttQueuePool, self.meterReadingController, useQ=self.useQ)        

    async def InitMqtt(self):
        self.mqttBrokers = _mqttBrokers
        self.mqttQueuePool.Initialise(self.mqttBrokers)

    async def PublishFromQueue(self, client, queue, publish_topic):
        while True:
            message = await queue.get()
            await client.publish(publish_topic, message, qos=1)

    async def RunPubMqttAsync(self, mqttBroker, pubQ):
        async with aiomqtt.Client(hostname = mqttBroker, identifier = str(uuid4())) as client:        
            print("Connected to:..." + mqttBroker)            
            await self.PublishFromQueue(client, pubQ, "/entities")

    async def InitDbConnPool(self, app):
        app['pool'] = await asyncpg.create_pool(database="pg_crud_test_server_py", user="postgres", password="1793", host="localhost")        

        yield

        app['pool'].close()

    async def GetPgDbConnectionPool(self):
        return await asyncpg.create_pool(database="pg_crud_test_server_py", user="postgres", password="1793", host="localhost")        

    async def InitSqliteDb(self, dbName):
        asyncDbConn = await aiosqlite.connect(dbName, check_same_thread=False)

        await asyncDbConn.execute("DROP TABLE IF EXISTS users")
        await asyncDbConn.execute('''CREATE TABLE IF NOT EXISTS users
                (USERNAME           TEXT    NOT NULL,
                PASSWORD    TEXT    NOT NULL);''')
        await asyncDbConn.execute('''CREATE UNIQUE INDEX index_username 
                            ON users(username)''')

        password = b64encode(bytes('bar'.encode("ascii"))).decode("ascii")
        await asyncDbConn.execute(f"INSERT INTO users (USERNAME, PASSWORD) \
            VALUES ('foo', '{password}')")

        await asyncDbConn.commit()
        return asyncDbConn

    async def defaultHandlerMiddleware(self, app, handler):
        async def middleware_handler(request):
            # Perform actions before the request is handled

            print(f"Default handler called for {request.method} {request.path}")
            
            # Call the original request handler
            response = await handler(request)

            # Perform actions after the request is handled
            print("Request processed")

            return response
        
        return middleware_handler

    async def Initializer(self) -> web.Application:
        # Setup routes and handlers
        self.app.router.add_post('/api/user', self.LoginHandler)
        self.app.router.add_delete('/api/user', self.LogoutHandler)

        # Items
        self.app.router.add_post('/api/todoitems/', self.AddToDoItem)
        self.app.router.add_put('/api/todoitems/{id}', self.UpdateToDoItem)
        self.app.router.add_get('/api/todoitems/{id}', self.GetToDoItem)
        self.app.router.add_get('/api/todoitems/count/', self.GetToDoItemCount)
        self.app.router.add_delete('/api/todoitems/{id}', self.DeleteToDoItem)        
        self.app.router.add_delete('/api/todoitems/', self.DeleteAllToDoItems)                

        # Assets
        self.app.router.add_post('/api/assets/', self.AddAsset)
        self.app.router.add_put('/api/assets/{id}', self.UpdateAsset)
        self.app.router.add_get('/api/assets/{id}', self.GetAsset)
        self.app.router.add_get('/api/assets/count/', self.GetAssetCount)
        self.app.router.add_delete('/api/assets/{id}', self.DeleteAsset)                
        self.app.router.add_delete('/api/assets/', self.DeleteAllAssets)                

        # Asset Tasks
        self.app.router.add_post('/api/assettasks/', self.AddAssetTask)
        self.app.router.add_put('/api/assettasks/{id}', self.UpdateAssetTask)
        self.app.router.add_get('/api/assettasks/{id}', self.GetAssetTask)
        self.app.router.add_get('/api/assettasks/count/', self.GetAssetTaskCount)
        self.app.router.add_delete('/api/assettasks/{id}', self.DeleteAssetTask)        
        self.app.router.add_delete('/api/assettasks/', self.DeleteAllAssetTasks)                

        # Meters
        self.app.router.add_post('/api/meters/', self.AddMeter)
        self.app.router.add_put('/api/meters/{id}', self.UpdateMeter)
        self.app.router.add_get('/api/meters/{id}', self.GetMeter)
        self.app.router.add_get('/api/meters/count/', self.GetMeterCount)
        self.app.router.add_delete('/api/meters/{id}', self.DeleteMeter)        
        self.app.router.add_delete('/api/meters/', self.DeleteAllMeters)                
        self.app.router.add_get('/api/meters/{id}/adr/', self.GetMeterAdr)        

        # Meter Readings
        self.app.router.add_post('/api/meterreadings/', self.AddMeterReading)
        self.app.router.add_put('/api/meterreadings/{id}', self.UpdateMeterReading)
        self.app.router.add_get('/api/meterreadings/{id}', self.GetMeterReading)
        self.app.router.add_get('/api/meterreadings/count/', self.GetMeterReadingCount)
        self.app.router.add_delete('/api/meterreadings/{id}', self.DeleteMeterReading)        
        self.app.router.add_delete('/api/meterreadings/', self.DeleteAllMeterReadings)                

        self.app.on_startup.append(self.InitDao)        
        #self.app.cleanup_ctx.append(self.BackgroundTasks)                        
        self.app.on_startup.append(self.BackgroundTasks)                                

        return self.app

    async def BackgroundTasks(self, app):
        await self.InitMqtt()                        

        for mqttBroker in self.mqttBrokers:        
            app[mqttBroker] = asyncio.create_task(self.RunPubMqttAsync(mqttBroker,
                                self.mqttQueuePool.GetPubQForBroker(mqttBroker)))
            
        #yield            

    async def LoginHandler(self, request: web.Request) -> web.Response:
        try:
            # loads dictionary from JSON-formatted request body
            header = request.headers.get('Authorization', None)

            # Authorization: Basic XXX
            kind, authorization = header.strip().split(' ', 1)

            if kind != "Basic":
                return web.HTTPBadRequest()

            authorization = base64_decode(authorization.strip()) \
                .decode('ascii') \
                .split(':')

            userName = authorization[0]
            password = authorization[1]

        except ValueError:
            return web.HTTPBadRequest()
        # if 'username' not in data or 'password' not in data:
        #     return web.HTTPUnprocessableEntity()
        # username = data['username']
        # password = data['password']

        if (self.ValidatePassword(userName, password) == False):
            return web.HTTPUnauthorized()

        sessionId = str(uuid4())
        self.userToSession[userName] = sessionId
        self.sessionToUser[sessionId] = userName
        response = {'session_id': sessionId}
        return web.json_response(response)

    def ValidatePassword(self, userName, password) -> web.Response:
        result = True
        dbConn = sqlite3.connect('test1.db')
        rawSql = f"SELECT password FROM users where username = '{userName}'"

        cursor = dbConn.execute(rawSql)

        dbPassword = None

        for row in cursor:
            dbPassword = row[0]

        if (dbPassword != None):
            dbPassword = base64_decode(dbPassword).decode('ascii')

        if (dbPassword == None or dbPassword != password):
            result = False

        return result

    async def LogoutHandler(self, request: web.Request) -> web.Response:
        authHeader = dict(request.headers).get('Authorization')
        sessionId = authHeader.strip().split(' ', 1)[1]

        if sessionId not in self.sessionToUser:
            return web.HTTPUnauthorized()

        username = self.sessionToUser[sessionId]
        self.sessionToUser.pop(sessionId)
        self.userToSession.pop(username)
        return web.HTTPOk()

    # ToDoItems
    async def AddToDoItem(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        item = json.loads(payload["itemData"])
        response = await self.toDoController.AddItem(payload["mqttSessionId"], item)        
        return web.json_response(response)

    async def UpdateToDoItem(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        item = json.loads(payload["itemData"])
        response = await self.toDoController.UpdateItem(payload["mqttSessionId"], id, item)
        if response is None:
            return web.HTTPNotFound()
        return web.json_response(response)

    async def GetToDoItem(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.toDoController.GetItemById(id)
        return web.json_response(response)

    async def DeleteToDoItem(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            itemData = await request.json()            
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.toDoController.DeleteItem(id, itemData["mqttSessionId"], itemData["messageId"])
        return web.json_response(response)

    async def DeleteAllToDoItems(self, request: web.Request) -> web.Response:
#        request.app['pool']        
        response = await self.toDoController.DeleteAllItems()
        return web.json_response(response)

    async def GetToDoItemCount(self, request: web.Request) -> web.Response:
        response = await self.toDoController.GetItemCount()
        return web.json_response(response)

    # Assets
    async def AddAsset(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        asset = json.loads(payload["assetData"])        
        response = await self.assetController.AddAsset(payload["mqttSessionId"], asset)
        return web.json_response(response)

    async def UpdateAsset(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            payload = await request.json()            
        except ValueError:
            return web.HTTPBadRequest()

        asset = json.loads(payload["assetData"])
        response = await self.assetController.UpdateAsset(payload["mqttSessionId"], id, asset)
        if response is None:
            return web.HTTPNotFound()
        return web.json_response(response)

    async def GetAsset(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.assetController.GetAssetById(id)
        return web.json_response(response)

    async def DeleteAsset(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            assetData = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.assetController.DeleteAsset(id, assetData["mqttSessionId"], assetData["messageId"])
        return web.json_response(response)

    async def DeleteAllAssets(self, request: web.Request) -> web.Response:
        response = await self.assetController.DeleteAllAssets()
        return web.json_response(response)

    async def GetAssetCount(self, request: web.Request) -> web.Response:
        response = await self.assetController.GetAssetCount()
        return web.json_response(response)

    # Meters
    async def AddMeter(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        meter = json.loads(payload["meterData"])        
        response = await self.meterController.AddMeter(payload["mqttSessionId"], meter)
        return web.json_response(response)

    async def UpdateMeter(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        meter = json.loads(payload["meterData"])
        response = await self.meterController.UpdateMeter(payload["mqttSessionId"], id, meter)

        if response is None:
            return web.HTTPNotFound()

        return web.json_response(response)

    async def GetMeter(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.meterController.GetMeterById(id)
        return web.json_response(response)

    async def GetMeterAdr(self, request: web.Request) -> web.Response:
        try:
            meterId = request.path.split('/')[3]                        
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.meterController.GetMeterAdr(meterId)
        return web.json_response(response)

    async def DeleteMeter(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            meterData = await request.json()            
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.meterController.DeleteMeter(id, meterData["mqttSessionId"], meterData["messageId"])
        return web.json_response(response)

    async def DeleteAllMeters(self, request: web.Request) -> web.Response:
        response = await self.meterController.DeleteAllMeters()
        return web.json_response(response)

    async def GetMeterCount(self, request: web.Request) -> web.Response:
        response = await self.meterController.GetMeterCount()
        return web.json_response(response)

    # Meter Readings
    async def AddMeterReading(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        meterReading = json.loads(payload["meterReadingData"])        
        response = await self.meterReadingController.AddMeterReading(payload["mqttSessionId"], meterReading)
        return web.json_response(response)

    async def UpdateMeterReading(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        meterReading = json.loads(payload["meterReadingData"])
        response = await self.meterReadingController.UpdateMeterReading(payload["mqttSessionId"], id, meterReading)
        if response is None:
            return web.HTTPNotFound()
        return web.json_response(response)

    async def GetMeterReading(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.meterReadingController.GetMeterReadingById(id)
        return web.json_response(response)

    async def DeleteMeterReading(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            meterReadingData = await request.json()                                    
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.meterReadingController.DeleteMeterReading(id, meterReadingData['mqttSessionId'], meterReadingData['messageId'])
        return web.json_response(response)

    async def DeleteAllMeterReadings(self, request: web.Request) -> web.Response:
        response = await self.meterReadingController.DeleteAllMeterReadings()
        return web.json_response(response)

    async def GetMeterReadingCount(self, request: web.Request) -> web.Response:
        response = await self.meterReadingController.GetMeterReadingCount()
        return web.json_response(response)

    # Asset Tasks
    async def AddAssetTask(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        assetTask = json.loads(payload["assetTaskData"])        
        response = await self.assetTaskController.AddAssetTask(payload["mqttSessionId"], assetTask)
        return web.json_response(response)

    async def UpdateAssetTask(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            payload = await request.json()
        except ValueError:
            return web.HTTPBadRequest()

        assetTask = json.loads(payload["assetTaskData"])
        response = await self.assetTaskController.UpdateAssetTask(payload["mqttSessionId"], id, assetTask)
        if response is None:
            return web.HTTPNotFound()
        return web.json_response(response)

    async def GetAssetTask(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.assetTaskController.GetAssetTaskById(id)
        return web.json_response(response)

    async def DeleteAssetTask(self, request: web.Request) -> web.Response:
        try:
            id = request.path.split('/')[3]                        
            assetTaskData = await request.json()                        
        except ValueError:
            return web.HTTPBadRequest()

        response = await self.assetTaskController.DeleteAssetTask(id, assetTaskData['mqttSessionId'], assetTaskData['messageId'])
        return web.json_response(response)

    async def DeleteAllAssetTasks(self, request: web.Request) -> web.Response:
        response = await self.assetTaskController.DeleteAllAssetTasks()
        return web.json_response(response)

    async def GetAssetTaskCount(self, request: web.Request) -> web.Response:
        response = await self.assetTaskController.GetAssetTaskCount()
        return web.json_response(response)

    def Run(self):
        web.run_app(self.Initializer(), host=self.host, port=self.port)

_mqttBrokers = ['192.168.10.124', '192.168.10.135']

if sys.platform.lower() == "win32" or os.name.lower() == "nt":
    from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

if __name__ == '__main__':
    with open('config.json') as fp:
        cfg = json.load(fp)

    webserver = WebServer(**cfg)
    webserver.Run()
