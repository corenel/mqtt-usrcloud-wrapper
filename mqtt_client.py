"""Wrapper class for Mqtt communication with UsrCloud."""

from __future__ import print_function

import paho.mqtt.client as mqtt


class MqttClient(object):
    """Client using MQTT protocol on UsrCloud server."""

    def __init__(self,
                 username="sdktest",
                 password="eab2a1fdcfdc1924b0dfd390a2dbabe2",
                 use_log=False,
                 host="clouddata.usr.cn",
                 port=8080,
                 timeout=600):
        """Init MqttClient."""
        super(MqttClient, self).__init__()

        self.username = username
        self.password = password
        self.use_log = use_log
        self.host = host
        self.port = port
        self.timeout = timeout

        # init client
        self.client = mqtt.Client(client_id="APP:{}".format(username),
                                  transport="websockets")
        # set username and password
        # note that password must be pre-proceed by md5
        self.client.username_pw_set(username=self.username,
                                    password=self.password)
        # set callback function
        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_subscribe = self.on_subscribe
        self.client.on_unsubscribe = self.on_unsubscribe
        if use_log:
            self.client.on_log = self.on_log

    def on_connect(self, client, userdata, flags, rc):
        """Callback funciton on connect."""
        print("connect: rc={}".format(str(rc)))

    def on_disconnect(self, client, userdata, rc):
        """Callback funciton on connect."""
        print("disconnect: rc={}".format(str(rc)))

    def on_message(self, client, userdata, msg):
        """Callback funciton on message."""
        print("message: [{}] qos={} msg={}".format(msg.topic,
                                                   msg.qos,
                                                   msg.payload))

    def on_publish(self, client, userdata, mid):
        """Callback funciton on publish."""
        print("publish: mid={}".format(str(mid)))

    def on_subscribe(self, client, userdata, mid, granted_qos):
        """Callback funciton on subscribe."""
        print("subscribe: mid={} qos={}".format(str(mid),
                                                str(granted_qos)))

    def on_unsubscribe(self, client, userdata, mid):
        """Callback funciton on unsubscribe."""
        print("unsubscribe: mid={}".format(str(mid)))

    def on_log(self, client, userdata, level, string):
        """Logger."""
        print(string)

    def connect(self):
        """Connect to server."""
        self.client.connect(self.host, self.port, self.timeout)

    def disconnect(self):
        """Disconnect from server."""
        self.client.disconnect()

    def subscribe(self, dev_id):
        """Subscribe to topic on UsrCloud."""
        topic = "$USR/DevTx/{}".format(dev_id)
        self.client.subscribe(topic, 0)

    def unsubscribe(self, dev_id):
        """Unsubscribe to topic on UsrCloud."""
        topic = "$USR/DevTx/{}".format(dev_id)
        self.client.unsubscribe(topic)

    def publish(self, dev_id, msg):
        """Publish message to specific dev on UsrCloud."""
        topic = "$USR/DevRx/{}".format(dev_id)
        msg_byte = bytearray()
        msg_byte.extend(map(ord, msg))
        print(msg_byte)
        self.client.publish(topic, msg_byte, qos=1)

    def loop_forever(self):
        """Loop client forever."""
        self.client.loop_forever()


if __name__ == '__main__':
    # Please replace username, password and device id as yours
    client = MqttClient()
    client.connect()
    client.subscribe("00007867000000000001")
    client.publish("00007867000000000001", b"\x01\x03\x00\x63\x00\x03\xf5\xd5")

    client.loop_forever()
