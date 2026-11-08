import paho.mqtt.client as mqtt

cliente = mqtt.Client()

cliente.connect("localhost", 1883)

cliente.publish("prueba", "hola desde Python")

cliente.disconnect() 