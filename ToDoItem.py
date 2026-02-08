from Entity import Entity

class ToDoItem(Entity):
    def __init__(self, id = 0, version = 0, clientId = "", messageId = 0, name = "", description = "", isComplete = False):
        super().__init__(id, version, description, clientId, messageId)        
        self.name = name
        self.isComplete = isComplete
