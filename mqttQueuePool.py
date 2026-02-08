from queue_1 import Queue
import random

class MqttQueuePool:
    def __init__(self):
        self.mqttQueuePool = {}                
        self.mqttBrokers = []
        self.brokerIndex = 0        

    def Initialise(self, mqttBrokers):
        self.mqttBrokers = mqttBrokers

        for mqttBroker in self.mqttBrokers:
            self.mqttQueuePool[mqttBroker] = Queue()            

    def GetPubQ(self):
        self.brokerIndex = random.randint(0, len(self.mqttBrokers) - 1)        

        if (self.brokerIndex > (len(self.mqttBrokers) - 1)):
            pubQ = self.mqttQueuePool[self.mqttBrokers[0]]                                                
        else:
            pubQ = self.mqttQueuePool[self.mqttBrokers[self.brokerIndex]]                                    
        
        return pubQ        

    def GetPubQForBroker(self, broker):
        if (broker in self.mqttBrokers):
            pubQ = self.mqttQueuePool[broker]                    
        else:
            pubQ = self.mqttQueuePool[self.mqttBrokers[0]]                    
        
        return pubQ        
