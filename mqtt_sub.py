import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    print(" connected with reslut", str(rc))
    client.subscribe("HELLO/WORLD")

def on_message(client, userdata, msg):
    print("{} {}".format(msg.topic, msg.payload))

client=mqtt.Client()
client.on_connect=on_connect
client.on_message=on_message

client.connect("iot.eclipse.org", 1883)
client.loop_forever()