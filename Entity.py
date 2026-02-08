class Entity:
    def __init__(self, id=0, version=0, description="", clientId = "", messageId = 0):
        self.id = id
        self.version = version
        self.description = description
        self.clientId = clientId        
        self.messageId = messageId
